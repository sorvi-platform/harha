const std = @import("std");
const harha = @import("../harha.zig");

allocator: std.mem.Allocator,
mnt: std.StringArrayHashMapUnmanaged(harha.Vfs),
dir: std.AutoArrayHashMapUnmanaged(harha.Dir, Dir),
file: std.AutoArrayHashMapUnmanaged(harha.File, File),
dir_id: u32,
file_id: u32,

const Dir = struct {
    vfs: harha.Vfs,
    path: []const u8,
    inner: harha.Dir,

    fn close(self: *@This(), allocator: std.mem.Allocator) void {
        self.vfs.closeDir(self.inner);
        allocator.free(self.path);
        self.* = undefined;
    }
};

const File = struct {
    vfs: harha.Vfs,
    inner: harha.File,

    fn close(self: *@This()) void {
        self.vfs.closeFile(self.inner);
        self.* = undefined;
    }
};

pub fn init(allocator: std.mem.Allocator) @This() {
    return .{
        .allocator = allocator,
        .mnt = .empty,
        .dir = .empty,
        .file = .empty,
        .dir_id = 1,
        .file_id = 0,
    };
}

pub fn mount(self: *@This(), fs: harha.Vfs, mnt_point: []const u8) !void {
    if (mnt_point.len == 0 or mnt_point[0] != '/') return error.RelativePath;
    for (self.mnt.values()) |other_fs| {
        // The overlay can't distinquish between 2 instances of same vfs so disallow the usage
        if (other_fs.ptr == fs.ptr) return error.VfsAlreadyMountedToOverlay;
    }
    const dupe = try self.allocator.dupe(u8, mnt_point);
    errdefer self.allocator.free(dupe);
    const res = try self.mnt.getOrPut(self.allocator, dupe);
    if (res.found_existing) return error.MountPointAlreadyExists;
    res.value_ptr.* = fs;
}

pub fn unmount(self: *@This(), mnt_point: []const u8) void {
    const mnt = self.mnt.fetchOrderedRemove(mnt_point) orelse return;
    for (0..self.file.count()) |i| {
        const idx = self.file.count() - i - 1;
        var file = self.file.values()[idx];
        if (file.vfs.ptr != mnt.value.ptr) continue;
        file.close();
        self.file.swapRemoveAt(idx);
    }
    for (0..self.dir.count()) |i| {
        const idx = self.dir.count() - i - 1;
        var dir = self.dir.values()[idx];
        if (dir.vfs.ptr != mnt.value.ptr) continue;
        dir.close(self.allocator);
        self.dir.swapRemoveAt(idx);
    }
    self.allocator.free(mnt.key);
}

pub fn deinit(self: *@This()) void {
    for (self.file.values()) |*file| file.close();
    self.file.deinit(self.allocator);
    for (self.dir.values()) |*dir| dir.close(self.allocator);
    self.dir.deinit(self.allocator);
    for (self.mnt.keys()) |path| self.allocator.free(path);
    self.mnt.deinit(self.allocator);
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

fn resolvedDir(self: *@This(), dir: harha.Dir) ?Dir {
    const root: Dir = .{ .vfs = self.vfs(.all), .inner = .root, .path = "" };
    if (dir == .root) return root;
    return self.dir.get(dir);
}

fn vfsForPath(self: *@This(), path: []const u8) ?struct { harha.Vfs, harha.SafePath } {
    const keys, const values = .{ self.mnt.keys(), self.mnt.values() };
    for (0..keys.len) |n| {
        const idx = keys.len - n - 1;
        if (std.mem.startsWith(u8, path, keys[idx])) {
            const child_off = @min(keys[idx].len + 1, path.len);
            return .{ values[idx], .{ .resolved = path[child_off..] } };
        }
    }
    return null;
}

fn openDir(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.Dir.OpenOptions) harha.OpenDirError!harha.Dir {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_dir = self.resolvedDir(dir) orelse return error.NotDir;
    const path: []const u8 = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{native_dir.path, sub_path});
    errdefer self.allocator.free(path);
    const fs, const child = self.vfsForPath(path) orelse return error.FileNotFound;
    try self.dir.ensureUnusedCapacity(self.allocator, 1);
    const res = try fs.openDir(native_dir.inner, child, options);
    errdefer comptime unreachable;
    defer self.dir_id +%= 1;
    self.dir.putAssumeCapacityNoClobber(@enumFromInt(self.dir_id), .{ .vfs = fs, .path = path, .inner = res });
    return @enumFromInt(self.dir_id);
}

fn closeDir(ptr: *anyopaque, dir: harha.Dir) void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    var native_dir = self.dir.fetchSwapRemove(dir) orelse return;
    native_dir.value.close(self.allocator);
}

fn deleteDir(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.Dir.DeleteOptions) harha.DeleteDirError!void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_dir = self.resolvedDir(dir) orelse return error.NotDir;
    const path: []const u8 = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{native_dir.path, sub_path});
    defer self.allocator.free(path);
    const fs, const child = self.vfsForPath(path) orelse return error.FileNotFound;
    return fs.deleteDir(native_dir.inner, child, options);
}

fn stat(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8) harha.StatError!harha.Stat {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    // TODO: Allow stating mount points under .root
    const native_dir = self.resolvedDir(dir) orelse return error.NotDir;
    const path: []const u8 = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{native_dir.path, sub_path});
    defer self.allocator.free(path);
    const fs, const child = self.vfsForPath(path) orelse return error.FileNotFound;
    return fs.stat(native_dir.inner, child);
}

fn iterate(ptr: *anyopaque, dir: harha.Dir) harha.IterateError!*anyopaque {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    // TODO: Allow iterating mount points under .root
    const native_dir = self.dir.getPtr(dir) orelse return error.NotOpenForIteration;
    // check permissions due to calling vtable function directly
    if (!native_dir.vfs.permissions.iterate) return error.PermissionDenied;
    return native_dir.vfs.vtable.iterate(native_dir.vfs.ptr, native_dir.inner);
}

fn iterateNext(ptr: *anyopaque, dir: harha.Dir, state: *anyopaque) harha.IterateError!?harha.Dir.Entry {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_dir = self.dir.getPtr(dir) orelse return error.NotOpenForIteration;
    std.debug.assert(native_dir.vfs.permissions.iterate);
    return native_dir.vfs.vtable.iterateNext(native_dir.vfs.ptr, native_dir.inner, state);
}

fn iterateReset(ptr: *anyopaque, dir: harha.Dir, state: *anyopaque) void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_dir = self.dir.getPtr(dir) orelse unreachable; // the directory the iterator is tied to is gone
    std.debug.assert(native_dir.vfs.permissions.iterate);
    native_dir.vfs.vtable.iterateReset(native_dir.vfs.ptr, native_dir.inner, state);
}

fn iterateDeinit(ptr: *anyopaque, dir: harha.Dir, state: *anyopaque) void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_dir = self.dir.getPtr(dir) orelse unreachable; // the directory the iterator is tied to is gone
    std.debug.assert(native_dir.vfs.permissions.iterate);
    native_dir.vfs.vtable.iterateDeinit(native_dir.vfs.ptr, native_dir.inner, state);
}

fn openFile(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.File.OpenOptions) harha.OpenFileError!harha.File {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_dir = self.resolvedDir(dir) orelse return error.NotDir;
    const path: []const u8 = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{native_dir.path, sub_path});
    defer self.allocator.free(path);
    const fs, const child = self.vfsForPath(path) orelse return error.FileNotFound;
    try self.file.ensureUnusedCapacity(self.allocator, 1);
    const res = try fs.openFile(native_dir.inner, child, options);
    errdefer comptime unreachable;
    defer self.file_id +%= 1;
    self.file.putAssumeCapacityNoClobber(@enumFromInt(self.file_id), .{ .vfs = fs, .inner = res });
    return @enumFromInt(self.file_id);
}

fn closeFile(ptr: *anyopaque, file: harha.File) void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    var native_file = self.file.fetchSwapRemove(file) orelse return;
    native_file.value.close();
}

fn deleteFile(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8) harha.DeleteFileError!void {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_dir = self.resolvedDir(dir) orelse return error.NotDir;
    const path: []const u8 = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{native_dir.path, sub_path});
    defer self.allocator.free(path);
    const fs, const child = self.vfsForPath(path) orelse return error.FileNotFound;
    return fs.deleteFile(native_dir.inner, child);
}

fn seek(ptr: *anyopaque, file: harha.File, offset: u64, whence: harha.File.Whence) harha.SeekError!u64 {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_file = self.file.getPtr(file) orelse return error.Unexpected;
    return native_file.vfs.seek(native_file.inner, offset, whence);
}

fn writev(ptr: *anyopaque, file: harha.File, iov: []const []const u8) harha.WriteError!usize {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_file = self.file.getPtr(file) orelse return error.Unexpected;
    return native_file.vfs.writev(native_file.inner, iov);
}

fn pwritev(ptr: *anyopaque, file: harha.File, iov: []const []const u8, offset: u64) harha.WriteError!usize {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_file = self.file.getPtr(file) orelse return error.Unexpected;
    return native_file.vfs.pwritev(native_file.inner, iov, offset);
}

fn readv(ptr: *anyopaque, file: harha.File, iov: []const []u8) harha.ReadError!usize {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_file = self.file.getPtr(file) orelse return error.Unexpected;
    return native_file.vfs.readv(native_file.inner, iov);
}

fn preadv(ptr: *anyopaque, file: harha.File, iov: []const []u8, offset: u64) harha.ReadError!usize {
    const self: *@This() = @ptrCast(@alignCast(ptr));
    const native_file = self.file.getPtr(file) orelse return error.Unexpected;
    return native_file.vfs.preadv(native_file.inner, iov, offset);
}
