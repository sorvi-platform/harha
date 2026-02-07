///! Harha virtual filesystem for SRA archives
///! <https://github.com/sorvi-platform/sra-archive>

const std = @import("std");
const harha = @import("../harha.zig");
const sra = @import("sra");

allocator: std.mem.Allocator,
archive: std.fs.File,
strings: []const u8,
path: std.StringArrayHashMapUnmanaged(Entry),
file: std.AutoArrayHashMapUnmanaged(harha.File, FileData),
generation: u11,

const Entry = struct {
    stat: harha.Stat,
    archive_offset: u64,
};

const FileData = struct {
    rw_offset: u64,
};

const Id = packed struct (u32) {
    kind: enum (u1) { dir, file },
    path_idx: u20,
    generation: u11,

    pub fn init(kind: harha.Kind, path_idx: u20, generation: u11) @This() {
        return .{
            .kind = @enumFromInt(@intFromEnum(kind)),
            .path_idx = path_idx,
            .generation = generation,
        };
    }

    pub fn fromHarhaDir(dir: harha.Dir) @This() {
        if (dir == .root) return .{ .kind = .dir, .path_idx = 0, .generation = 0 };
        return @bitCast(@as(u32, @intFromEnum(dir)));
    }

    pub fn toHarhaDir(self: @This()) harha.Dir {
        std.debug.assert(self.kind == .dir);
        return @enumFromInt(@as(u32, @bitCast(self)));
    }

    pub fn fromHarhaFile(dir: harha.File) @This() {
        return @bitCast(@as(u32, @intFromEnum(dir)));
    }

    pub fn toHarhaFile(self: @This()) harha.File {
        std.debug.assert(self.kind == .file);
        return @enumFromInt(@as(u32, @bitCast(self)));
    }

    pub fn path(self: @This(), map: *const std.StringArrayHashMapUnmanaged(Entry)) []const u8 {
        return map.keys()[self.path_idx];
    }

    pub fn entry(self: @This(), map: *const std.StringArrayHashMapUnmanaged(Entry)) Entry {
        return map.values()[self.path_idx];
    }
};

fn strForOffset(offset: usize, strings: []const u8) []const u8 {
    return std.mem.sliceTo(strings[offset..], 0);
}

pub fn init(allocator: std.mem.Allocator, file: std.fs.File) !@This() {
    var buffer: [4096]u8 = undefined;
    var file_reader = file.reader(&buffer);
    var reader: sra.Reader = try .init(&file_reader);
    try reader.validate();

    const strings = try reader.readStringTableAlloc(allocator);
    errdefer allocator.free(strings);

    var map: std.StringArrayHashMapUnmanaged(Entry) = .empty;
    errdefer map.deinit(allocator);

    // Root directory
    try map.putNoClobber(allocator, "", .{.archive_offset = 0, .stat = .{
        .size = 0,
        .kind = .dir,
        .mtime = 0,
        .ctime = 0,
    }});

    // Build file entries
    {
        var iter = try reader.iterator();
        try map.ensureUnusedCapacity(allocator, iter.num_entries);
        while (try iter.next(&reader)) |entry| {
            try reader.validateEntry(entry);
            const path_offset: u32 = @intCast(entry.path_offset - reader.path_table_offset);
            const path = strForOffset(path_offset, strings);
            map.putAssumeCapacityNoClobber(path, .{
                .stat = .{
                    .kind = .file,
                    .size = entry.data_length,
                    .ctime = entry.data_mtime * std.time.ns_per_ms,
                    .mtime = entry.data_mtime * std.time.ns_per_ms,
                },
                .archive_offset = entry.data_offset,
            });
        }
    }

    // Build dir entries (reuses the file path strings)
    var set: std.StringArrayHashMapUnmanaged(void) = .empty;
    defer set.deinit(allocator);
    {
        var iter = std.mem.tokenizeScalar(u8, strings, 0);
        while (iter.next()) |path| {
            try sra.validatePath(path);
            var iter2 = std.mem.tokenizeScalar(u8, std.fs.path.dirnamePosix(path) orelse "", '/');
            var path_offset: u32 = 0;
            while (iter2.next()) |part| {
                path_offset += @intCast(part.len);
                const res = try map.getOrPut(allocator, path[0..path_offset]);
                if (!res.found_existing) {
                    res.value_ptr.* = .{
                        .stat = .{
                            .kind = .dir,
                            .size = 0,
                            .ctime = 0,
                            .mtime = 0,
                        },
                        .archive_offset = 0,
                    };
                }
                path_offset += 1;
            }
        }
    }

    return .{
        .allocator = allocator,
        .archive = file,
        .strings = strings,
        .path = map,
        .file = .empty,
        .generation = 0,
    };
}

pub fn initPath(allocator: std.mem.Allocator, dir: std.fs.Dir, sub_path: []const u8) !@This() {
    var archive = try dir.openFile(sub_path, .{});
    errdefer archive.close();
    return try init(allocator, archive);
}

pub fn deinit(self: *@This()) void {
    self.archive.close();
    self.path.deinit(self.allocator);
    self.file.deinit(self.allocator);
    self.allocator.free(self.strings);
    self.* = undefined;
}

pub fn vfs(self: *@This(), permissions: harha.Permissions) harha.Vfs {
    return .{
        .ptr = self,
        .vtable = &.{
            .openDir = openDir,
            .closeDir = harha.noop.closeDir,
            .deleteDir = harha.noop.deleteDir,
            .stat = stat,
            .iterate = iterate,
            .iterateNext = iterateNext,
            .iterateReset = iterateReset,
            .iterateDeinit = iterateDeinit,
            .openFile = openFile,
            .closeFile = closeFile,
            .deleteFile = harha.noop.deleteFile,
            .seek = seek,
            .writev = harha.noop.writev,
            .pwritev = harha.noop.pwritev,
            .readv = readv,
            .preadv = preadv,
        },
        .permissions = permissions,
    };
}

fn lookupPathIndex(self: *@This(), parent_path: []const u8, sub_path: []const u8) !?u20 {
    if (parent_path.len == 0) {
        const idx = self.path.getIndex(sub_path) orelse return null;
        return @intCast(idx);
    }
    const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{parent_path, sub_path});
    defer self.allocator.free(path);
    const idx = self.path.getIndex(path) orelse return null;
    return @intCast(idx);
}

fn lookupPathEntry(self: *@This(), parent_path: []const u8, sub_path: []const u8) !?*Entry {
    if (parent_path.len == 0) return self.path.getPtr(sub_path);
    if (sub_path.len == 0) return self.path.getPtr(parent_path);
    const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{parent_path, sub_path});
    defer self.allocator.free(path);
    return self.path.getPtr(path);
}

fn openDir(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.Dir.OpenOptions) harha.OpenDirError!harha.Dir {
    if (options.create) return error.PermissionDenied;
    const id: Id = .fromHarhaDir(dir);
    if (id.kind != .dir) return error.NotDir;
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const pidx = try self.lookupPathIndex(id.path(&self.path), sub_path) orelse return error.FileNotFound;
    if (self.path.values()[pidx].stat.kind != .dir) return error.NotDir;
    errdefer comptime unreachable;
    defer self.generation +%= 1;
    return Id.init(.dir, pidx, self.generation).toHarhaDir();
}

fn stat(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8) harha.StatError!harha.Stat {
    const id: Id = .fromHarhaDir(dir);
    if (id.kind != .dir) return error.NotDir;
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const entry = try self.lookupPathEntry(id.path(&self.path), sub_path) orelse return error.FileNotFound;
    return entry.stat;
}

const Iterator = struct {
    entries: []const harha.Dir.Entry,
    index: usize,
};

fn iterate(ptr: *anyopaque, dir: harha.Dir) harha.IterateError!*anyopaque {
    const id: Id = .fromHarhaDir(dir);
    if (id.kind != .dir) return error.NotOpenForIteration;
    const self: *@This() = @ptrCast(@alignCast(ptr));
    var entries: std.ArrayList(harha.Dir.Entry) = .empty;
    errdefer entries.deinit(self.allocator);
    const parent_path = id.path(&self.path);
    for (self.path.keys(), self.path.values()) |path, *entry| {
        if (path.len <= parent_path.len) continue;
        if (parent_path.len > 0 and !std.mem.startsWith(u8, path, parent_path)) continue;
        const child_path = if (parent_path.len > 0) path[parent_path.len + 1..] else path;
        if (std.mem.indexOfScalar(u8, child_path, '/')) |_| continue;
        try entries.append(self.allocator, .{
            .basename = std.fs.path.basenamePosix(path),
            .stat = entry.stat,
        });
    }
    const iter = try self.allocator.create(Iterator);
    errdefer self.allocator.destroy(iter);
    iter.* = .{
        .entries = try entries.toOwnedSlice(self.allocator),
        .index = 0,
    };
    return iter;
}

fn iterateNext(_: *anyopaque, dir: harha.Dir, state: *anyopaque) harha.IterateError!?harha.Dir.Entry {
    const id: Id = .fromHarhaDir(dir);
    if (id.kind != .dir) return error.NotOpenForIteration;
    var iter: *Iterator = @ptrCast(@alignCast(state));
    if (iter.index >= iter.entries.len) return null;
    defer iter.index += 1;
    return iter.entries[iter.index];
}

fn iterateReset(_: *anyopaque, dir: harha.Dir, state: *anyopaque) void {
    const id: Id = .fromHarhaDir(dir);
    std.debug.assert(id.kind == .dir);
    var iter: *Iterator = @ptrCast(@alignCast(state));
    iter.index = 0;
}

fn iterateDeinit(ptr: *anyopaque, dir: harha.Dir, state: *anyopaque) void {
    const id: Id = .fromHarhaDir(dir);
    std.debug.assert(id.kind == .dir);
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const iter: *Iterator = @ptrCast(@alignCast(state));
    self.allocator.free(iter.entries);
    self.allocator.destroy(iter);
}

fn openFile(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.File.OpenOptions) harha.OpenFileError!harha.File {
    if (options.create or options.mode == .write_only or options.mode == .read_write) return error.PermissionDenied;
    const id: Id = .fromHarhaDir(dir);
    if (id.kind != .dir) return error.NotDir;
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const pidx = try self.lookupPathIndex(id.path(&self.path), sub_path) orelse return error.FileNotFound;
    const entry = self.path.values()[pidx];
    if (entry.stat.kind != .file) return error.IsDir;
    try self.file.ensureUnusedCapacity(self.allocator, 1);
    errdefer comptime unreachable;
    defer self.generation +%= 1;
    const fd: Id = .init(.file, pidx, self.generation);
    self.file.putAssumeCapacityNoClobber(fd.toHarhaFile(), .{ .rw_offset = 0 });
    return fd.toHarhaFile();
}

fn closeFile(ptr: *anyopaque, file: harha.File) void {
    const id: Id = .fromHarhaFile(file);
    if (id.kind != .file) return;
    const self: *@This() = @ptrCast(@alignCast(ptr));
    _ = self.file.swapRemove(file);
}

fn seek(ptr: *anyopaque, file: harha.File, offset: u64, whence: harha.File.Whence) harha.SeekError!u64 {
    const id: Id = .fromHarhaFile(file);
    if (id.kind != .file) return error.Unexpected;
    const self: *@This() = @ptrCast(@alignCast(ptr));
    var native_file = self.file.getPtr(file) orelse return error.Unexpected;
    switch (whence) {
        .set => native_file.rw_offset = offset,
        .forward => native_file.rw_offset +|= offset,
        .backward => native_file.rw_offset -|= offset,
        .from_end => {
            const entry = id.entry(&self.path);
            native_file.rw_offset = entry.stat.size -| offset;
        },
    }
    return native_file.rw_offset;
}

fn innerReadv(file: std.fs.File, iov: []const []u8, initial_offset: u64, limit: u64) !usize {
    var offset: u64 = initial_offset;
    var bytes_left: usize = limit;
    var consumed: usize = 0;
    var iovs_left = iov.len;
    while (iovs_left > 0) {
        var piov: [16]std.posix.iovec = undefined;
        const piov_len = @min(piov.len, iovs_left);
        var batch_bytes: usize = 0;
        for (0..piov_len) |idx| {
            piov[idx].base = iov[iov.len - iovs_left].ptr;
            piov[idx].len = @min(iov[iov.len - iovs_left].len, bytes_left);
            batch_bytes += piov[idx].len;
            bytes_left -= piov[idx].len;
            iovs_left -= 1;
        }
        const consumed_batch = file.preadv(piov[0..piov_len], offset) catch |err| return switch (err) {
            error.AccessDenied => error.PermissionDenied,
            error.Unseekable,
            error.InputOutput,
            error.SystemResources,
            error.IsDir,
            error.OperationAborted,
            error.BrokenPipe,
            error.ConnectionResetByPeer,
            error.ConnectionTimedOut,
            error.SocketNotConnected,
            error.WouldBlock,
            error.Canceled,
            error.ProcessNotFound,
            error.LockViolation => error.Unexpected,
            else => |e| return e,
        };
        consumed += consumed_batch;
        if (consumed_batch < batch_bytes) {
            return consumed; // partial read!
        }
        offset += consumed_batch;
    }
    return consumed;
}

fn readv(ptr: *anyopaque, file: harha.File, iov: []const []u8) harha.ReadError!usize {
    const id: Id = .fromHarhaFile(file);
    if (id.kind != .file) return error.NotOpenForReading;
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_file = self.file.getPtr(file) orelse return error.NotOpenForReading;
    const entry = id.entry(&self.path);
    const consumed = try innerReadv(self.archive, iov, entry.archive_offset + native_file.rw_offset, entry.stat.size -| native_file.rw_offset);
    native_file.rw_offset += consumed;
    return consumed;
}

fn preadv(ptr: *anyopaque, file: harha.File, iov: []const []u8, offset: u64) harha.ReadError!usize {
    const id: Id = .fromHarhaFile(file);
    if (id.kind != .file) return error.NotOpenForReading;
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const entry = id.entry(&self.path);
    return innerReadv(self.archive, iov, entry.archive_offset + offset, entry.stat.size -| offset);
}
