const std = @import("std");
const harha = @import("../harha.zig");

allocator: std.mem.Allocator,
root: std.fs.Dir,
managed_root: bool,
dir: std.AutoArrayHashMapUnmanaged(harha.Dir, DirData),
file: std.AutoArrayHashMapUnmanaged(harha.File, FileData),

// Technically this isn't required, but we want to be able to
// cleanup all the opened dir handles, and also check that the
// application is not trying to operate on dir handles it did
// not open.
const DirData = struct {
    os: std.fs.Dir,

    fn init(os: std.fs.Dir) @This() {
        return .{ .os = os };
    }

    fn close(self: *@This()) void {
        self.os.close();
        self.* = undefined;
    }

    fn toHarha(self: *const @This()) harha.Dir {
        const int: u32 = (switch (@typeInfo(@TypeOf(self.os.fd))) {
            .int => @intCast(self.os.fd),
            else => @intFromPtr(self.os.fd),
        });
        std.debug.assert(int != 0);
        return @enumFromInt(int);
    }
};

const FileData = struct {
    os: std.fs.File,
    // don't rely on OS for this
    rw_offset: u64,

    fn init(os: std.fs.File) @This() {
        return .{
            .os = os,
            .rw_offset = 0,
        };
    }

    fn close(self: *@This()) void {
        self.os.close();
        self.* = undefined;
    }

    fn toHarha(self: *const @This()) harha.File {
        return @enumFromInt(
            (switch (@typeInfo(@TypeOf(self.os.handle))) {
                .int => self.os.handle,
                else => @intFromPtr(self.os.handle),
            })
        );
    }
};

pub fn init(allocator: std.mem.Allocator, root_dir: std.fs.Dir) !@This() {
    return .{
        .allocator = allocator,
        .root = root_dir,
        .managed_root = false,
        .dir = .empty,
        .file = .empty,
    };
}

pub fn initPath(allocator: std.mem.Allocator, dir: std.fs.Dir, sub_path: []const u8) !@This() {
    const root = try dir.openDir(sub_path, .{.iterate = true});
    errdefer root.close();
    return .{
        .allocator = allocator,
        .root = root,
        .managed_root = true,
        .dir = .empty,
        .file = .empty,
    };
}

pub fn deinit(self: *@This()) void {
    for (self.file.values()) |*file| file.close();
    self.file.deinit(self.allocator);
    for (self.dir.values()) |*dir| dir.close();
    self.dir.deinit(self.allocator);
    if (self.managed_root) self.root.close();
    self.* = undefined;
}

pub fn vfs(self: *@This(), permissions: harha.Permissions) harha.Vfs {
    return .{
        .ptr = self,
        .vtable = &.{
            .openDir = openDir,
            .closeDir = closeDir,
            .deleteDir = deleteDir,
            .stat = stat,
            .iterate = iterate,
            .iterateNext = iterateNext,
            .iterateReset = iterateReset,
            .iterateDeinit = iterateDeinit,
            .openFile = openFile,
            .closeFile = closeFile,
            .deleteFile = deleteFile,
            .seek = seek,
            .writev = writev,
            .pwritev = pwritev,
            .readv = readv,
            .preadv = preadv,
        },
        .permissions = permissions,
    };
}

fn toOsDir(self: *const @This(), dir: harha.Dir) ?std.fs.Dir {
    return switch (dir) {
        .root => self.root,
        else => D: {
            const native_dir = self.dir.get(dir) orelse return null;
            break :D native_dir.os;
        },
    };
}

fn openDir(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.Dir.OpenOptions) harha.OpenDirError!harha.Dir {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const os_dir = toOsDir(self, dir) orelse return error.NotDir;
    try self.dir.ensureUnusedCapacity(self.allocator, 1);
    const actual_sub_path = if (sub_path.len > 0) sub_path else ".";
    const res_dir: std.fs.Dir = switch (options.create) {
        true => os_dir.makeOpenPath(actual_sub_path, .{.no_follow = true, .iterate = options.iterate}),
        false => os_dir.openDir(actual_sub_path, .{.no_follow = true, .iterate = options.iterate}),
    } catch |err| return switch (err) {
        error.AccessDenied,
        error.ReadOnlyFileSystem => error.PermissionDenied,
        error.SystemResources,
        error.ProcessFdQuotaExceeded,
        error.SystemFdQuotaExceeded,
        error.FileTooBig,
        error.NoSpaceLeft,
        error.DiskQuota,
        error.LinkQuotaExceeded => error.ResourceLimitReached,
        error.IsDir,
        error.WouldBlock,
        error.ProcessNotFound,
        error.SharingViolation,
        error.PipeBusy,
        error.NoDevice,
        error.NameTooLong,
        error.InvalidUtf8,
        error.InvalidWtf8,
        error.BadPathName,
        error.NetworkNotFound,
        error.AntivirusInterference,
        error.SymLinkLoop,
        error.DeviceBusy,
        error.FileLocksNotSupported,
        error.FileBusy => error.Unexpected,
        else => |e| e,
    };
    errdefer comptime unreachable;
    const res: DirData = .init(res_dir);
    self.dir.putAssumeCapacityNoClobber(res.toHarha(), res);
    return res.toHarha();
}

fn closeDir(ptr: *anyopaque, dir: harha.Dir) void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    switch (dir) {
        .root => return,
        else => {
            var kv = self.dir.fetchSwapRemove(dir) orelse return;
            kv.value.close();
        }
    }
}

fn deleteDir(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.Dir.DeleteOptions) harha.DeleteDirError!void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const os_dir = toOsDir(self, dir) orelse return error.NotDir;
    const actual_sub_path = if (sub_path.len > 0) sub_path else ".";
    _ = switch (options.recursive) {
        true => os_dir.deleteTree(actual_sub_path),
        false => os_dir.deleteDir(actual_sub_path),
    } catch |err| return switch (err) {
        error.AccessDenied,
        error.ReadOnlyFileSystem => error.PermissionDenied,
        error.SystemResources,
        error.ProcessNotFound,
        error.NoDevice,
        error.NameTooLong,
        error.InvalidUtf8,
        error.InvalidWtf8,
        error.BadPathName,
        error.NetworkNotFound,
        error.SymLinkLoop,
        error.ProcessFdQuotaExceeded,
        error.SystemFdQuotaExceeded,
        error.FileTooBig,
        error.FileBusy,
        error.DeviceBusy,
        error.FileSystem => error.Unexpected,
        else => |e| e,
    };
}

fn toHarhaStat(st: std.fs.File.Stat) harha.Stat {
    return .{
        .kind = switch (st.kind) {
            .directory => .dir,
            .file => .file,
            else => unreachable,
        },
        .mtime = st.mtime,
        .ctime = st.ctime,
        .size = st.size,
    };
}

fn stat(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8) harha.StatError!harha.Stat {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const os_dir = toOsDir(self, dir) orelse return error.NotDir;
    const actual_sub_path = if (sub_path.len > 0) sub_path else ".";
    const st = os_dir.statFile(actual_sub_path) catch |err| return switch (err) {
        error.AccessDenied => error.PermissionDenied,
        error.SystemResources,
        error.ProcessFdQuotaExceeded,
        error.SystemFdQuotaExceeded,
        error.FileTooBig,
        error.NoSpaceLeft,
        error.IsDir,
        error.WouldBlock,
        error.ProcessNotFound,
        error.SharingViolation,
        error.PipeBusy,
        error.NoDevice,
        error.NameTooLong,
        error.InvalidUtf8,
        error.InvalidWtf8,
        error.BadPathName,
        error.NetworkNotFound,
        error.AntivirusInterference,
        error.SymLinkLoop,
        error.DeviceBusy,
        error.FileLocksNotSupported,
        error.FileBusy,
        error.PathAlreadyExists => error.Unexpected,
        else => |e| e,
    };
    return toHarhaStat(st);
}

fn iterate(ptr: *anyopaque, dir: harha.Dir) harha.IterateError!*anyopaque {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const os_dir = toOsDir(self, dir) orelse return error.Unexpected;
    const iter = try self.allocator.create(std.fs.Dir.Iterator);
    iter.* = os_dir.iterate();
    return iter;
}

fn iterateNext(_: *anyopaque, _: harha.Dir, state: *anyopaque) harha.IterateError!?harha.Dir.Entry {
    const iter: *std.fs.Dir.Iterator = @ptrCast(@alignCast(state));
    while (true) {
        const entry = (iter.next() catch |err| return switch (err) {
            error.AccessDenied => error.PermissionDenied,
            error.SystemResources,
            error.InvalidUtf8 => error.Unexpected,
            else => |e| e,
        }) orelse break;
        harha.SafePath.validate(entry.name) catch continue;
        const st = iter.dir.statFile(entry.name) catch |err| return switch (err) {
            error.FileNotFound => continue,
            error.AccessDenied => error.PermissionDenied,
            error.NotDir,
            error.SystemResources,
            error.ProcessFdQuotaExceeded,
            error.SystemFdQuotaExceeded,
            error.FileTooBig,
            error.NoSpaceLeft,
            error.IsDir,
            error.WouldBlock,
            error.ProcessNotFound,
            error.SharingViolation,
            error.PipeBusy,
            error.NoDevice,
            error.NameTooLong,
            error.InvalidUtf8,
            error.InvalidWtf8,
            error.BadPathName,
            error.NetworkNotFound,
            error.AntivirusInterference,
            error.SymLinkLoop,
            error.DeviceBusy,
            error.FileLocksNotSupported,
            error.FileBusy,
            error.PathAlreadyExists => error.Unexpected,
            else => |e| e,
        };
        switch (st.kind) {
            .directory, .file => return .{
                .basename = entry.name,
                .stat = toHarhaStat(st),
            },
            else => continue,
        }
    }
    return null;
}

fn iterateReset(_: *anyopaque, _: harha.Dir, state: *anyopaque) void {
    const iter: *std.fs.Dir.Iterator = @ptrCast(@alignCast(state));
    iter.reset();
}

fn iterateDeinit(ptr: *anyopaque, _: harha.Dir, state: *anyopaque) void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const iter: *std.fs.Dir.Iterator = @ptrCast(@alignCast(state));
    self.allocator.destroy(iter);
}

fn openFile(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.File.OpenOptions) harha.OpenFileError!harha.File {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const os_dir = toOsDir(self, dir) orelse return error.NotDir;
    try self.file.ensureUnusedCapacity(self.allocator, 1);
    const actual_sub_path = if (sub_path.len > 0) sub_path else ".";
    const res_file: std.fs.File = switch (options.create) {
        true => os_dir.createFile(actual_sub_path, .{
            .read = options.mode == .read_only or options.mode == .read_write,
        }),
        false => os_dir.openFile(actual_sub_path, switch (options.mode) {
            .read_only => .{ .mode = .read_only },
            .write_only => .{ .mode = .write_only },
            .read_write => .{ .mode = .read_write },
        }),
    } catch |err| return switch (err) {
        error.AccessDenied => error.PermissionDenied,
        error.SystemResources,
        error.ProcessFdQuotaExceeded,
        error.SystemFdQuotaExceeded,
        error.FileTooBig,
        error.NoSpaceLeft => error.ResourceLimitReached,
        error.WouldBlock,
        error.ProcessNotFound,
        error.SharingViolation,
        error.PipeBusy,
        error.NoDevice,
        error.NameTooLong,
        error.InvalidUtf8,
        error.InvalidWtf8,
        error.BadPathName,
        error.NetworkNotFound,
        error.AntivirusInterference,
        error.SymLinkLoop,
        error.DeviceBusy,
        error.FileLocksNotSupported,
        error.FileBusy => error.Unexpected,
        else => |e| e,
    };
    errdefer res_file.close();
    const st = res_file.stat() catch |err| return switch (err) {
        error.AccessDenied => error.PermissionDenied,
        else => error.Unexpected,
    };
    if (st.kind != .file) return error.IsDir;
    errdefer comptime unreachable;
    const res: FileData = .init(res_file);
    self.file.putAssumeCapacityNoClobber(res.toHarha(), res);
    return res.toHarha();
}

fn closeFile(ptr: *anyopaque, file: harha.File) void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    var kv = self.file.fetchSwapRemove(file) orelse return;
    kv.value.close();
}

fn deleteFile(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8) harha.DeleteFileError!void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const os_dir = toOsDir(self, dir) orelse return error.NotDir;
    const actual_sub_path = if (sub_path.len > 0) sub_path else ".";
    os_dir.deleteFile(actual_sub_path) catch |err| return switch (err) {
        error.AccessDenied,
        error.ReadOnlyFileSystem => error.PermissionDenied,
        error.SystemResources,
        error.NameTooLong,
        error.InvalidUtf8,
        error.InvalidWtf8,
        error.BadPathName,
        error.NetworkNotFound,
        error.SymLinkLoop,
        error.FileBusy,
        error.FileSystem => error.Unexpected,
        else => |e| e,
    };
}

fn seek(ptr: *anyopaque, file: harha.File, cursor: harha.File.Cursor) harha.SeekError!u64 {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    var native_file = self.file.getPtr(file) orelse return error.Unexpected;
    switch (cursor) {
        .set => |offset| native_file.rw_offset = offset,
        .forward => |delta| native_file.rw_offset +|= delta,
        .backward => |delta| native_file.rw_offset -|= delta,
        .from_end => |delta| native_file.rw_offset = (native_file.os.getEndPos() catch |err| return switch (err) {
            error.AccessDenied => error.PermissionDenied,
            error.SystemResources => error.Unexpected,
            else => |e| e,
        }) -| delta,
    }
    return native_file.rw_offset;
}

fn innerWritev(file: std.fs.File, iov: []const []const u8, initial_offset: u64) !usize {
    var offset: u64 = initial_offset;
    var written: usize = 0;
    var iovs_left = iov.len;
    while (iovs_left > 0) {
        var piov: [16]std.posix.iovec_const = undefined;
        const piov_len = @min(piov.len, iovs_left);
        var batch_bytes: usize = 0;
        for (0..piov_len) |idx| {
            piov[idx].base = iov[iov.len - iovs_left].ptr;
            piov[idx].len = iov[iov.len - iovs_left].len;
            batch_bytes += piov[idx].len;
            iovs_left -= 1;
        }
        const written_batch = file.pwritev(piov[0..piov_len], offset) catch |err| return switch (err) {
            error.FileTooBig,
            error.MessageTooBig,
            error.DiskQuota => error.NoSpaceLeft,
            error.AccessDenied => error.PermissionDenied,
            error.NoDevice,
            error.DeviceBusy,
            error.InvalidArgument,
            error.Unseekable,
            error.InputOutput,
            error.SystemResources,
            error.OperationAborted,
            error.BrokenPipe,
            error.ConnectionResetByPeer,
            error.WouldBlock,
            error.ProcessNotFound,
            error.LockViolation => error.Unexpected,
            else => |e| return e,
        };
        written += written_batch;
        if (written_batch < batch_bytes) {
            return written; // partial write!
        }
        offset += written_batch;
    }
    return written;
}

fn writev(ptr: *anyopaque, file: harha.File, iov: []const []const u8) harha.WriteError!usize {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    var native_file = self.file.getPtr(file) orelse return error.NotOpenForWriting;
    const written = try innerWritev(native_file.os, iov, native_file.rw_offset);
    native_file.rw_offset += written;
    return written;
}

fn pwritev(ptr: *anyopaque, file: harha.File, iov: []const []const u8, offset: u64) harha.WriteError!usize {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_file = self.file.get(file) orelse return error.NotOpenForWriting;
    return innerWritev(native_file.os, iov, offset);
}

fn innerReadv(file: std.fs.File, iov: []const []u8, initial_offset: u64) !usize {
    var offset: u64 = initial_offset;
    var consumed: usize = 0;
    var iovs_left = iov.len;
    while (iovs_left > 0) {
        var piov: [16]std.posix.iovec = undefined;
        const piov_len = @min(piov.len, iovs_left);
        var batch_bytes: usize = 0;
        for (0..piov_len) |idx| {
            piov[idx].base = iov[iov.len - iovs_left].ptr;
            piov[idx].len = iov[iov.len - iovs_left].len;
            batch_bytes += piov[idx].len;
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
    const self: *@This() = @ptrCast(@alignCast(ptr));
    var native_file = self.file.getPtr(file) orelse return error.NotOpenForReading;
    const consumed = try innerReadv(native_file.os, iov, native_file.rw_offset);
    native_file.rw_offset += consumed;
    return consumed;
}

fn preadv(ptr: *anyopaque, file: harha.File, iov: []const []u8, offset: u64) harha.ReadError!usize {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_file = self.file.get(file) orelse return error.NotOpenForReading;
    return try innerReadv(native_file.os, iov, offset);
}
