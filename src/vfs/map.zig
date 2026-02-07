const std = @import("std");
const harha = @import("../harha.zig");

/// This VFS maps enum to different kind of VFS
/// It's kind of like Overlay expect it does not have file system hierarchy itself
///
/// This VFS bitpacks the enum index and the inner Dir/File handle to the  returned Dir/File handle.
/// The bitpacking is done so that `.root` (aka 0) corresponds to the first VFS root,
/// 1 to second VFS root, 2 to third VFS root, and so on.
///
/// Due to both inner VFS file/dir handle and the enum index being bitpacked to single handle
/// using this with large enums may not be viable. The larger the enum the less files/dirs
/// can be opened.
///
/// On upside, this VFS is very simple and almost directly talks to the underlying VFSes.
/// This VFS requires zero allocation.
pub fn Map(E: type) type {
    for (std.enums.values(E), 0..) |e, idx| {
        if (@intFromEnum(e) != idx) {
            @compileError("harha.Map only works with 0..N range enums");
        }
    }

    const vfs_enum_len = std.enums.values(E).len;
    const BackingInt = std.math.IntFittingRange(0, vfs_enum_len);

    const Id = packed struct (u32) {
        mnt_idx: BackingInt,
        inner: std.meta.Int(.unsigned, 32 - @typeInfo(BackingInt).int.bits),

        pub fn initInnerDir(mnt_idx: BackingInt, inner: harha.Dir) @This() {
            return .{
                .mnt_idx = mnt_idx,
                .inner = @intCast(@intFromEnum(inner)),
            };
        }

        pub fn initInnerFile(mnt_idx: BackingInt, inner: harha.File) @This() {
            return .{
                .mnt_idx = mnt_idx,
                .inner = @intCast(@intFromEnum(inner)),
            };
        }

        pub fn fromHarhaDir(dir: harha.Dir) @This() {
            return @bitCast(@as(u32, @intFromEnum(dir)));
        }

        pub fn toHarhaDir(self: @This()) harha.Dir {
            return @enumFromInt(@as(u32, @bitCast(self)));
        }

        pub fn fromHarhaFile(dir: harha.File) @This() {
            return @bitCast(@as(u32, @intFromEnum(dir)));
        }

        pub fn toHarhaFile(self: @This()) harha.File {
            return @enumFromInt(@as(u32, @bitCast(self)));
        }

        pub fn innerDir(self: @This()) harha.Dir {
            return @enumFromInt(self.inner);
        }

        pub fn innerFile(self: @This()) harha.File {
            return @enumFromInt(self.inner);
        }
    };

    return struct {
        mnt: [vfs_enum_len]?harha.Vfs,

        pub const init: @This() = .{ .mnt = @splat(null) };

        /// Returns `harha.Dir` which points to the root of specific sub-vfs
        pub fn rootDir(_: *const @This(), root: E) harha.Dir {
            return Id.initInnerDir(@intCast(@intFromEnum(root)), .root).toHarhaDir();
        }

        pub fn mount(self: *@This(), root: E, fs: harha.Vfs) void {
            self.mnt[@intFromEnum(root)] = fs;
        }

        pub fn unmount(self: *@This(), root: E) void {
            self.mnt[@intFromEnum(root)] = null;
        }

        pub fn deinit(self: *@This()) void {
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

        fn openDir(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.Dir.OpenOptions) harha.OpenDirError!harha.Dir {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotDir;
            const new_dir = try fs.openDir(id.innerDir(), .{ .resolved = sub_path }, options);
            return Id.initInnerDir(id.mnt_idx, new_dir).toHarhaDir();
        }

        fn closeDir(ptr: *anyopaque, dir: harha.Dir) void {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse return;
            fs.closeDir(id.innerDir());
        }

        fn deleteDir(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.Dir.DeleteOptions) harha.DeleteDirError!void {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotDir;
            try fs.deleteDir(id.innerDir(), .{ .resolved = sub_path }, options);
        }

        fn stat(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8) harha.StatError!harha.Stat {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotDir;
            return fs.stat(id.innerDir(), .{ .resolved = sub_path });
        }

        fn iterate(ptr: *anyopaque, dir: harha.Dir) harha.IterateError!*anyopaque {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotOpenForIteration;
            const iter = try fs.iterate(id.innerDir());
            return iter.ptr;
        }

        fn iterateNext(ptr: *anyopaque, dir: harha.Dir, state: *anyopaque) harha.IterateError!?harha.Dir.Entry {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse unreachable; // vfs went somewhere during iteration
            const rdir: harha.Dir = if (fs.root != .root and id.innerDir() == .root) fs.root else id.innerDir();
            std.debug.assert(fs.permissions.iterate);
            return fs.vtable.iterateNext(fs.ptr, rdir, state);
        }

        fn iterateReset(ptr: *anyopaque, dir: harha.Dir, state: *anyopaque) void {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse unreachable; // vfs went somewhere during iteration
            const rdir: harha.Dir = if (fs.root != .root and id.innerDir() == .root) fs.root else id.innerDir();
            std.debug.assert(fs.permissions.iterate);
            return fs.vtable.iterateReset(fs.ptr, rdir, state);
        }

        fn iterateDeinit(ptr: *anyopaque, dir: harha.Dir, state: *anyopaque) void {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse unreachable; // vfs went somewhere during iteration
            const rdir: harha.Dir = if (fs.root != .root and id.innerDir() == .root) fs.root else id.innerDir();
            std.debug.assert(fs.permissions.iterate);
            return fs.vtable.iterateDeinit(fs.ptr, rdir, state);
        }

        fn openFile(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8, options: harha.File.OpenOptions) harha.OpenFileError!harha.File {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotDir;
            const new_file = try fs.openFile(id.innerDir(), .{ .resolved = sub_path }, options);
            return Id.initInnerFile(id.mnt_idx, new_file).toHarhaFile();
        }

        fn closeFile(ptr: *anyopaque, file: harha.File) void {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaFile(file);
            const fs = self.mnt[id.mnt_idx] orelse return;
            fs.closeFile(id.innerFile());
        }

        fn deleteFile(ptr: *anyopaque, dir: harha.Dir, sub_path: []const u8) harha.DeleteFileError!void {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaDir(dir);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotDir;
            try fs.deleteFile(id.innerDir(), .{ .resolved = sub_path });
        }

        fn seek(ptr: *anyopaque, file: harha.File, offset: u64, whence: harha.File.Whence) harha.SeekError!u64 {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaFile(file);
            const fs = self.mnt[id.mnt_idx] orelse return error.Unseekable;
            return fs.seek(id.innerFile(), offset, whence);
        }

        fn writev(ptr: *anyopaque, file: harha.File, iov: []const []const u8) harha.WriteError!usize {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaFile(file);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotOpenForWriting;
            return fs.writev(id.innerFile(), iov);
        }

        fn pwritev(ptr: *anyopaque, file: harha.File, iov: []const []const u8, offset: u64) harha.WriteError!usize {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaFile(file);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotOpenForWriting;
            return fs.pwritev(id.innerFile(), iov, offset);
        }

        fn readv(ptr: *anyopaque, file: harha.File, iov: []const []u8) harha.ReadError!usize {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaFile(file);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotOpenForReading;
            return fs.readv(id.innerFile(), iov);
        }

        fn preadv(ptr: *anyopaque, file: harha.File, iov: []const []u8, offset: u64) harha.ReadError!usize {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            const id: Id = .fromHarhaFile(file);
            const fs = self.mnt[id.mnt_idx] orelse return error.NotOpenForReading;
            return fs.preadv(id.innerFile(), iov, offset);
        }
    };
}
