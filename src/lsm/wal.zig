const std = @import("std");
const log = @import("../log.zig");
const MemTable = @import("memtable.zig").MemTable;
const builtin = @import("builtin");

const RecordFormat = @import("wal/record.zig").RecordFormat;

pub const WAL = struct {
    file: std.Io.File,
    allocator: std.mem.Allocator,
    io: std.Io,
    sync_mode: SyncMode,
    mutex: std.Io.Mutex,
    cond: std.Io.Condition,

    // Group Commit fields
    buffer: std.ArrayListUnmanaged(u8),
    flush_buffer: std.ArrayListUnmanaged(u8), // Buffer used for background flushing

    current_sync_version: u64, // Persisted on disk
    max_buffered_version: u64, // In memory (buffer)

    is_flushing: bool,
    flushing_version: u64, // The version being flushed right now
    pending_sqe_count: u32, // How many SQEs are in flight for this flush

    // io_uring fields
    ring: if (builtin.os.tag == .linux) std.os.linux.IoUring else void,
    use_uring: bool,
    file_offset: u64,

    pub const SyncMode = enum {
        Full,
        Normal,
        Off,
    };

    pub fn init(allocator: std.mem.Allocator, path: []const u8, sync_mode: SyncMode, io: std.Io) !WAL {
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .truncate = false });
        const end_pos = try file.length(io);

        var self = WAL{
            .file = file,
            .allocator = allocator,
            .io = io,
            .sync_mode = sync_mode,
            .mutex = .init,
            .cond = .init,
            .buffer = .empty,
            .flush_buffer = .empty,
            .current_sync_version = 0,
            .max_buffered_version = 0,
            .is_flushing = false,
            .flushing_version = 0,
            .pending_sqe_count = 0,
            .ring = undefined,
            .use_uring = false,
            .file_offset = end_pos,
        };

        if (builtin.os.tag == .linux) {
            // Initialize io_uring with a small queue depth
            self.ring = std.os.linux.IoUring.init(32, 0) catch |err| {
                log.warn(.wal, "Failed to init io_uring: {}, falling back to sync I/O", .{err});
                return self;
            };
            self.use_uring = true;
        }

        return self;
    }

    pub fn deinit(self: *WAL) void {
        if (self.buffer.items.len > 0) {
            self.performSyncFlush(self.buffer.items) catch {};
        }
        if (builtin.os.tag == .linux and self.use_uring) {
            self.ring.deinit();
        }
        self.file.close(self.io);
        self.buffer.deinit(self.allocator);
        self.flush_buffer.deinit(self.allocator);
    }

    // Helper to serialize data into the buffer
    pub fn serializeEntry(buffer: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, key: []const u8, value: []const u8, version: u64) !void {
        try RecordFormat.encode(buffer, allocator, key, value, version);
    }

    pub fn appendRaw(self: *WAL, raw_data: []const u8, version: u64) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        try self.buffer.appendSlice(self.allocator, raw_data);

        if (version > self.max_buffered_version) {
            self.max_buffered_version = version;
        }
    }

    fn performSyncFlush(self: *WAL, data: []const u8) !void {
        try self.file.writePositionalAll(self.io, data, self.file_offset);

        if (self.sync_mode == .Full) {
            try self.file.sync(self.io);
        }
    }

    // Async Pipelining API
    pub fn submitFlush(self: *WAL, version: u64) !void {
        _ = version;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.is_flushing) return error.FlushInProgress;

        // Swap buffers
        const tmp = self.buffer;
        self.buffer = self.flush_buffer;
        self.flush_buffer = tmp;

        self.is_flushing = true;
        self.flushing_version = self.max_buffered_version;
        self.pending_sqe_count = 0;

        const bytes_to_write = self.flush_buffer.items;
        if (bytes_to_write.len == 0) {
            self.is_flushing = false;
            self.current_sync_version = self.flushing_version;
            return;
        }

        if (builtin.os.tag == .linux and self.use_uring) {
            const link_fsync = (self.sync_mode == .Full);
            const submitted = self.submitWriteAsync(bytes_to_write, link_fsync) catch |err| {
                log.warn(.wal, "Async WAL Submit Failed: {}. Falling back to Sync.", .{err});
                try self.performSyncFlush(bytes_to_write);
                self.file_offset += bytes_to_write.len;
                self.is_flushing = false;
                self.current_sync_version = self.flushing_version;
                self.flush_buffer.clearRetainingCapacity();
                return;
            };

            self.pending_sqe_count = submitted;
            self.file_offset += bytes_to_write.len;
        } else {
            try self.performSyncFlush(bytes_to_write);
            self.file_offset += bytes_to_write.len;
            self.is_flushing = false;
            self.current_sync_version = self.flushing_version;
            self.flush_buffer.clearRetainingCapacity();
        }
    }

    pub fn completeFlush(self: *WAL) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (!self.is_flushing) return;

        if (builtin.os.tag == .linux and self.use_uring) {
            const wait_cnt = self.pending_sqe_count;
            if (wait_cnt > 0) {
                _ = try self.ring.submit_and_wait(wait_cnt);

                var i: u32 = 0;
                while (i < wait_cnt) : (i += 1) {
                    const cqe = try self.ring.copy_cqe();
                    if (cqe.res < 0) {
                        return error.IoUringOpFailed;
                    }
                }
            }
        }

        self.current_sync_version = self.flushing_version;
        self.is_flushing = false;
        self.flush_buffer.clearRetainingCapacity();
        self.cond.broadcast(self.io);
    }

    fn submitWriteAsync(self: *WAL, data: []const u8, link_fsync: bool) !u32 {
        var sqe_count: u32 = 1;

        // 1. Write
        const sqe_write = self.ring.get_sqe() catch return error.RingFull;
        sqe_write.opcode = std.os.linux.IORING_OP.WRITE;
        sqe_write.fd = self.file.handle;
        sqe_write.addr = @intFromPtr(data.ptr);
        sqe_write.len = @intCast(data.len);
        sqe_write.off = self.file_offset;
        sqe_write.user_data = 1;

        if (link_fsync) {
            sqe_write.flags |= std.os.linux.IOSQE_IO_LINK;

            // 2. Fsync
            const sqe_fsync = self.ring.get_sqe() catch return error.RingFull;
            sqe_fsync.opcode = std.os.linux.IORING_OP.FSYNC;
            sqe_fsync.fd = self.file.handle;
            sqe_fsync.off = 0; // Ignored for fsync
            sqe_fsync.len = 0; // Ignored
            sqe_fsync.user_data = 2;
            // No flags (flags = 0 usually implies IORING_FSYNC_DATASYNC if set, but we want standard behavior or 0)
            // std.os.linux.IORING_FSYNC_DATASYNC is an option but Full sync implies metadata too usually.

            sqe_count += 1;
        }

        _ = try self.ring.submit();
        return sqe_count;
    }

    pub fn waitForDurability(self: *WAL, version: u64) !void {
        self.mutex.lockUncancelable(self.io);

        if (self.current_sync_version >= version) {
            self.mutex.unlock(self.io);
            return;
        }
        self.mutex.unlock(self.io);

        try self.submitFlush(version);
        try self.completeFlush();
    }

    pub fn replay(self: *WAL, memtable: *MemTable, min_version: u64) !u64 {
        var pos: u64 = 0;
        var max_version: u64 = 0;

        // Reusable buffer for keys and values to avoid small allocs
        var temp_buffer: std.ArrayListUnmanaged(u8) = .empty;
        defer temp_buffer.deinit(self.allocator);

        while (true) {
            const record_start = pos;
            // Header: CRC(4), Ver(8), KLen(4), VLen(4) = 20 bytes
            var header: [20]u8 = undefined;
            const n_head = try self.file.readPositionalAll(self.io, &header, record_start);
            if (n_head == 0) break; // Clean EOF
            if (n_head < 20) {
                try self.file.setLength(self.io, record_start);
                pos = record_start;
                break;
            }
            pos = record_start + 20;

            const crc = std.mem.readInt(u32, header[0..4], .little);
            const version = std.mem.readInt(u64, header[4..12], .little);
            if (version > max_version) max_version = version;

            const key_len = std.mem.readInt(u32, header[12..16], .little);
            const val_len = std.mem.readInt(u32, header[16..20], .little);

            try temp_buffer.resize(self.allocator, key_len + val_len);

            // Read payload
            const n_payload = try self.file.readPositionalAll(self.io, temp_buffer.items, pos);
            if (n_payload < temp_buffer.items.len) {
                try self.file.setLength(self.io, record_start);
                pos = record_start;
                break;
            }
            pos += n_payload;

            const key = temp_buffer.items[0..key_len];
            const val = temp_buffer.items[key_len..][0..val_len];

            // Verify CRC
            var hash = std.hash.Crc32.init();
            var buf8: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf8, version, .little);
            hash.update(&buf8);

            var tmp: [4]u8 = undefined;
            std.mem.writeInt(u32, &tmp, key_len, .little);
            hash.update(&tmp);
            std.mem.writeInt(u32, &tmp, val_len, .little);
            hash.update(&tmp);
            hash.update(key);
            hash.update(val);

            if (hash.final() != crc) return error.CorruptWAL;

            // Idempotency Check: Only apply if version > min_version
            if (version > min_version) {
                try memtable.put(key, val, version);
            }
        }

        self.file_offset = pos;

        self.current_sync_version = max_version;
        self.max_buffered_version = max_version;

        // If max_version < min_version, it means the WAL is completely stale (or empty).
        // We should ensure we return at least min_version to keep logical clock monotonic.
        if (max_version < min_version) max_version = min_version;

        return max_version;
    }
};

test "WAL group commit flow" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const test_path = "test_gc_groupcommit.wal";

    std.Io.Dir.cwd().deleteFile(io, test_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, test_path) catch {};

    var wal = try WAL.init(allocator, test_path, .Full, io);
    defer wal.deinit();

    var buf1: std.ArrayListUnmanaged(u8) = .empty;
    defer buf1.deinit(allocator);
    try WAL.serializeEntry(&buf1, allocator, "k1", "v1", 10);

    var buf2: std.ArrayListUnmanaged(u8) = .empty;
    defer buf2.deinit(allocator);
    try WAL.serializeEntry(&buf2, allocator, "k2", "v2", 11);

    // T1 Appends
    try wal.appendRaw(buf1.items, 10);

    // T2 Appends
    try wal.appendRaw(buf2.items, 11);

    // T1 Waits (Triggers Sync)
    // Note: In single thread test, this will flush both.
    try wal.waitForDurability(10);

    // T2 Waits (Should return immediately)
    try wal.waitForDurability(11);

    // Verify Replay
    var wal2 = try WAL.init(allocator, test_path, .Full, io);
    defer wal2.deinit();
    var memtable = try MemTable.init(allocator, io);
    defer memtable.deinit();

    const max_ver = try wal2.replay(memtable, 0);

    try std.testing.expectEqual(@as(u64, 11), max_ver);
    if (memtable.get("k1", 20)) |v| {
        try std.testing.expectEqualStrings("v1", v);
    } else try std.testing.expect(false);

    if (memtable.get("k2", 20)) |v| {
        try std.testing.expectEqualStrings("v2", v);
    } else try std.testing.expect(false);
}

test "WAL recovery with partial write" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const test_path = "test_wal_partial_recovery.log";

    std.Io.Dir.cwd().deleteFile(io, test_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, test_path) catch {};

    // 1. Write valid data
    {
        var wal = try WAL.init(allocator, test_path, .Full, io);
        defer wal.deinit();

        var batch: std.ArrayListUnmanaged(u8) = .empty;
        defer batch.deinit(allocator);

        // Entry 1 (v10)
        // CRC(4) + Ver(8) + KL(4) + VL(4) + K + V
        const k = "key1";
        const v = "val1";
        // We use the public helper to avoid manual struct layout assumptions which might drift
        try WAL.serializeEntry(&batch, allocator, k, v, 10);

        try wal.appendRaw(batch.items, 10);
        try wal.waitForDurability(10);
    }

    // 2. Append garbage
    {
        const file = try std.Io.Dir.cwd().openFile(io, test_path, .{ .mode = .read_write });
        defer file.close(io);
        const end_pos = try file.length(io);
        // Append partial header (less than 4 bytes CRC, or partial fields)
        // A CRC is 4 bytes. If we write 3 bytes, replay loop checks `n < 4` and breaks safely.
        // Let's write enough to pass first check but fail later (e.g., CRC OK, but partial key).
        // Wait, if we write random garbage, CRC check will fail.
        // The goal is "partial write", meaning sudden power loss.
        // This usually looks like a truncated record at the end.
        const garbage = "par";
        try file.writePositionalAll(io, garbage, end_pos);
    }

    // 3. Replay
    {
        var wal = try WAL.init(allocator, test_path, .Full, io);
        defer wal.deinit();

        var mem = try MemTable.init(allocator, io);
        defer mem.deinit();

        const max_ver = try wal.replay(mem, 0);

        try std.testing.expectEqual(max_ver, 10);
        if (mem.get("key1", 100)) |val| {
            try std.testing.expectEqualStrings("val1", val);
        } else {
            try std.testing.expect(false);
        }

        const expected_record_size: u64 = RecordFormat.HEADER_SIZE + "key1".len + "val1".len;
        const stat_after_replay = try wal.file.stat(io);
        try std.testing.expectEqual(expected_record_size, stat_after_replay.size);

        var batch2: std.ArrayListUnmanaged(u8) = .empty;
        defer batch2.deinit(allocator);
        try WAL.serializeEntry(&batch2, allocator, "key2", "val2", 11);
        try wal.appendRaw(batch2.items, 11);
        try wal.waitForDurability(11);

        var wal3 = try WAL.init(allocator, test_path, .Full, io);
        defer wal3.deinit();
        var mem2 = try MemTable.init(allocator, io);
        defer mem2.deinit();

        const max_ver2 = try wal3.replay(mem2, 0);
        try std.testing.expectEqual(@as(u64, 11), max_ver2);

        if (mem2.get("key1", 100)) |val| {
            try std.testing.expectEqualStrings("val1", val);
        } else {
            try std.testing.expect(false);
        }
        if (mem2.get("key2", 100)) |val| {
            try std.testing.expectEqualStrings("val2", val);
        } else {
            try std.testing.expect(false);
        }

        const expected_total_size: u64 = expected_record_size + RecordFormat.HEADER_SIZE + "key2".len + "val2".len;
        const stat_after_append = try wal3.file.stat(io);
        try std.testing.expectEqual(expected_total_size, stat_after_append.size);
    }
}
