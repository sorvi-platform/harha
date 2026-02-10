///! Harha is a portable virtual filesystem API
///! It allows implementing and using different kind of filesystems using a single API
///! Harha does not support many OS level concepts like symlinks etc...

const std = @import("std");
const enabled = @import("build_options");
pub const Overlay = @import("vfs/Overlay.zig");
pub const Map = @import("vfs/map.zig").Map;
pub const Std = @import("vfs/Std.zig");
pub const Sra = if (enabled.sra) @import("vfs/Sra.zig") else @compileError("sra archive support was not enabled");

pub const VTable = struct {
    openDir: *const fn (*anyopaque, dir: Dir, sub_path: []const u8, options: Dir.OpenOptions) OpenDirError!Dir,
    closeDir: *const fn (*anyopaque, dir: Dir) void,
    deleteDir: *const fn (*anyopaque, dir: Dir, sub_path: []const u8, options: Dir.DeleteOptions) DeleteDirError!void,
    stat: *const fn (*anyopaque, dir: Dir, sub_path: []const u8) StatError!Stat,
    iterate: *const fn (*anyopaque, dir: Dir) IterateError!*anyopaque,
    iterateNext: *const fn (*anyopaque, dir: Dir, state: *anyopaque) IterateError!?Dir.Entry,
    iterateReset: *const fn (*anyopaque, dir: Dir, state: *anyopaque) void,
    iterateDeinit: *const fn (*anyopaque, dir: Dir, state: *anyopaque) void,
    openFile: *const fn (*anyopaque, dir: Dir, sub_path: []const u8, options: File.OpenOptions) OpenFileError!File,
    closeFile: *const fn (*anyopaque, file: File) void,
    deleteFile: *const fn (*anyopaque, dir: Dir, sub_path: []const u8) DeleteFileError!void,
    seek: *const fn (*anyopaque, file: File, offset: u64, whence: File.Whence) SeekError!u64,
    writev: *const fn (*anyopaque, file: File, iov: []const []const u8) WriteError!usize,
    pwritev: *const fn (*anyopaque, file: File, iov: []const []const u8, offset: u64) WriteError!usize,
    readv: *const fn (*anyopaque, file: File, iov: []const []u8) ReadError!usize,
    preadv: *const fn (*anyopaque, file: File, iov: []const []u8, offset: u64) ReadError!usize,
};

pub const Vfs = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    permissions: Permissions,
    root: Dir = .root,

    /// Change the root directory for this interface.
    /// This is completely transparent to the filesystem implementation.
    /// Using this function in combination of throwaway interface instances may leak directory descriptors!
    /// Filesystem implementations do clean up after themselves on deinit however.
    pub fn chroot(self: *@This(), dir: Dir, sub_path: SafePath) !void {
        const rdir: Dir = if (sub_path.isAbsolute()) .root else dir;
        if (self.root != .root) self.closeDir(self.root);
        if (sub_path.relative().len > 0) {
            self.root = try self.vtable.openDir(self.ptr, rdir, sub_path.relative(), .{.iterate = true});
        } else {
            self.root = .root;
        }
    }

    pub fn openDir(self: @This(), dir: Dir, sub_path: SafePath, options: Dir.OpenOptions) !Dir {
        if (!self.permissions.create and options.create) return error.PermissionDenied;
        const rdir: Dir = if (dir == .root or sub_path.isAbsolute()) self.root else dir;
        return self.vtable.openDir(self.ptr, rdir, sub_path.relative(), options);
    }

    pub fn closeDir(self: @This(), dir: Dir) void {
        self.vtable.closeDir(self.ptr, dir);
    }

    pub fn deleteDir(self: @This(), dir: Dir, sub_path: SafePath, options: Dir.DeleteOptions) !void {
        if (!self.permissions.delete) return error.PermissionDenied;
        const rdir: Dir = if (dir == .root or sub_path.isAbsolute()) self.root else dir;
        return self.vtable.deleteDir(self.ptr, rdir, sub_path.relative(), options);
    }

    pub fn stat(self: @This(), dir: Dir, sub_path: SafePath) !Stat {
        if (!self.permissions.stat) return error.PermissionDenied;
        const rdir: Dir = if (dir == .root or sub_path.isAbsolute()) self.root else dir;
        return self.vtable.stat(self.ptr, rdir, sub_path.relative());
    }

    pub fn iterate(self: @This(), dir: Dir) !Iterator {
        if (!self.permissions.iterate) return error.PermissionDenied;
        const rdir: Dir = if (dir == .root) self.root else dir;
        return .{
            .vfs = self,
            .dir = rdir,
            .ptr = try self.vtable.iterate(self.ptr, rdir),
        };
    }

    pub fn walkSelectively(self: @This(), dir: Dir, allocator: std.mem.Allocator) !SelectiveWalker {
        const rdir: Dir = if (dir == .root) self.root else dir;
        var stack: std.ArrayList(SelectiveWalker.StackItem) = .empty;
        try stack.append(allocator, .{
            .iter = try self.iterate(rdir),
            .dirname_len = 0,
        });
        return .{
            .stack = stack,
            .name_buffer = .{},
            .allocator = allocator,
        };
    }

    pub fn walk(self: @This(), dir: Dir, allocator: std.mem.Allocator) !Walker {
        const rdir: Dir = if (dir == .root) self.root else dir;
        return .{ .inner = try self.walkSelectively(rdir, allocator) };
    }

    pub fn openFile(self: @This(), dir: Dir, sub_path: SafePath, options: File.OpenOptions) !File {
        if (!self.permissions.create and options.create) return error.PermissionDenied;
        if (!self.permissions.read and (options.mode == .read_only or options.mode == .read_write)) return error.PermissionDenied;
        if (!self.permissions.write and (options.mode == .write_only or options.mode == .read_write)) return error.PermissionDenied;
        const rdir: Dir = if (dir == .root or sub_path.isAbsolute()) self.root else dir;
        return self.vtable.openFile(self.ptr, rdir, sub_path.relative(), options);
    }

    pub fn closeFile(self: @This(), file: File) void {
        self.vtable.closeFile(self.ptr, file);
    }

    pub fn deleteFile(self: @This(), dir: Dir, sub_path: SafePath) !void {
        if (!self.permissions.delete) return error.PermissionDenied;
        const rdir: Dir = if (dir == .root or sub_path.isAbsolute()) self.root else dir;
        return self.vtable.deleteFile(self.ptr, rdir, sub_path.relative());
    }

    pub fn seek(self: @This(), file: File, offset: u64, whence: File.Whence) !u64 {
        if (!self.permissions.stat) return error.PermissionDenied;
        return self.vtable.seek(self.ptr, file, offset, whence);
    }

    pub fn writev(self: @This(), file: File, iov: []const []const u8) !usize {
        if (!self.permissions.write) return error.PermissionDenied;
        return self.vtable.writev(self.ptr, file, iov);
    }

    pub fn pwritev(self: @This(), file: File, iov: []const []const u8, offset: u64) !usize {
        if (!self.permissions.write) return error.PermissionDenied;
        return self.vtable.pwritev(self.ptr, file, iov, offset);
    }

    pub fn readv(self: @This(), file: File, iov: []const []u8) !usize {
        if (!self.permissions.read) return error.PermissionDenied;
        return self.vtable.readv(self.ptr, file, iov);
    }

    pub fn preadv(self: @This(), file: File, iov: []const []u8, offset: u64) !usize {
        if (!self.permissions.read) return error.PermissionDenied;
        return self.vtable.preadv(self.ptr, file, iov, offset);
    }
};

pub const Kind = enum {
    dir,
    file,
};

pub const Stat = struct {
    kind: Kind,
    mtime: i128,
    ctime: i128,
    size: u64,
};

pub const Dir = enum (u32) {
    root,
    _,

    pub const OpenOptions = struct {
        /// If set to true, iteration operation can be performed on the directory
        iterate: bool = false,
        /// If the path does not exist, the path (including any missing parents) will be created
        create: bool = false,
    };

    pub const DeleteOptions = struct {
        /// Delete the whole underlying directory tree
        recursive: bool = false,
    };

    pub const Entry = struct {
        basename: []const u8,
        stat: Stat,
    };
};

pub const File = enum (u32) {
    _,

    pub const Mode = enum {
        read_only,
        write_only,
        read_write,
    };

    pub const OpenOptions = struct {
        mode: Mode = .read_only,
        /// If the path does not exist, the path (including any missing parents) will be created
        create: bool = false,
    };

    pub const Whence = enum {
        set,
        forward,
        backward,
        from_end,
    };
};

pub const Permissions = packed struct {
    /// Allow creation of new files and directories
    create: bool,
    /// Allow deletion of files and directories
    delete: bool,
    /// Allow reading from files
    read: bool,
    /// Allow writing to files
    write: bool,
    /// Allow directory iteration
    iterate: bool,
    /// Allow stat and seek
    stat: bool,

    pub const all: @This() = .read_write;
    pub const read_write: @This() = .{ .create = true, .delete = true, .read = true, .write = true, .iterate = true, .stat = true };
    pub const read_only: @This() = .{ .create = false, .delete = false, .read = true, .write = false, .iterate = true, .stat = true };
    pub const write_only: @This() = .{ .create = true, .delete = true, .read = false, .write = true, .iterate = false, .stat = false };
};

pub const OpenDirError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    OutOfMemory,
    FileNotFound,
    NotDir,
    PathAlreadyExists,
    ResourceLimitReached,
};

pub const DeleteDirError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    OutOfMemory,
    FileNotFound,
    NotDir,
    DirNotEmpty,
};

pub const StatError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    OutOfMemory,
    FileNotFound,
    NotDir,
};

pub const IterateError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    OutOfMemory,
    NotOpenForIteration,
};

pub const OpenFileError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    OutOfMemory,
    FileNotFound,
    NotDir,
    IsDir,
    PathAlreadyExists,
    ResourceLimitReached,
};

pub const DeleteFileError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    OutOfMemory,
    FileNotFound,
    NotDir,
    IsDir,
};

pub const SeekError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    Unseekable,
};

pub const WriteError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    NotOpenForWriting,
    NoSpaceLeft,
};

pub const ReadError = error {
    Unexpected,
    Unsupported,
    PermissionDenied,
    NotOpenForReading,
};

/// Unfortunately some platforms (like Windows) don't have O_RESOLVE_BENEATH.
/// While this prevents path traversal attacks it also makes the vfs implementations
/// simpler as they don't have to deal with paths containing traversal or otherwise invalid components.
///
/// This won't actually translate paths runtime to absolute paths.
/// Translating paths would require allocation, thus if the user of this library wants to support
/// runtime path traversal, they have to implement the translation themself.
///
/// The paths Harha considers safe may not be compatible with the native filesystem:
/// * All paths must be valid UTF8.
/// * '/' is the path separator.
/// * '.' and '..' path components are not allowed.
/// * '/' character, control characters and whitespace other than the space are not allowed in path components.
/// * This means paths may not contain newlines, tabs, and such.A
/// * In addition to be nice to Windows, following characters are not allowed: [<>:"/\|?*]
pub const SafePath = struct {
    resolved: []const u8,

    const Traversal = enum {
        @".",
        @"..",
    };

    const Error = error {
        InvalidPath,
    };

    pub fn validate(path: []const u8) !void {
        if (std.mem.indexOf(u8, path, "//")) |_| return error.InvalidPath;
        if (path.len > 1 and path[path.len - 1] == '/') return error.InvalidPath;
        if (std.mem.indexOfAny(u8, path, "<>:\"\\|?*")) |_| return error.InvalidPath;
        if (!std.unicode.utf8ValidateSlice(path)) return error.InvalidPath;
        var iter = std.mem.tokenizeScalar(u8, path, '/');
        while (iter.next()) |part| {
            if (std.meta.stringToEnum(Traversal, part)) |_| return error.InvalidPath;
            for (part) |c| {
                if (c == '/') return error.InvalidPath;
                if (std.ascii.isControl(c)) return error.InvalidPath;
                if (c != ' ' and std.ascii.isWhitespace(c)) return error.InvalidPath;
            }
        }
    }

    /// Resolve the unsafe path comptime.
    /// This supports translating the unsafe path to absolute path as no runtime allocation is required.
    pub fn resolveComptime(comptime unsafe_path: []const u8) Error!@This() {
        // no comptime allocators still so can't reuse resolve
        // <https://github.com/ziglang/zig/issues/14931>
        return comptime D: {
            if (unsafe_path.len == 0) break :D .{ .resolved = "" };
            var components: []const []const u8 = &.{};

            var iter = std.mem.tokenizeScalar(u8, unsafe_path, '/');
            while (iter.next()) |component| {
                if (std.meta.stringToEnum(Traversal, component)) |t| {
                    switch (t) {
                        .@"." => {},
                        .@".." => if (components.len > 0) {
                            components = components[0..components.len - 1];
                        },
                    }
                } else {
                    components = components ++ .{component};
                }
            }

            if (components.len == 0) {
                break :D error.InvalidPath;
            }

            const is_absolute = unsafe_path.len > 0 and unsafe_path[0] == '/';
            var resolved: []const u8 = components[0];
            if (components.len > 1) {
                for (components[1..]) |component| resolved = resolved ++ "/" ++ component;
            }
            if (is_absolute) resolved = "/" ++ resolved;
            try validate(resolved);
            break :D .{ .resolved = resolved };
        };
    }

    pub fn resolve(unsafe_path: []const u8) Error!@This() {
        try validate(unsafe_path);
        return .{ .resolved = unsafe_path };
    }

    pub fn isAbsolute(self: @This()) bool {
        return self.resolved.len > 0 and self.resolved[0] == '/';
    }

    pub fn relative(self: @This()) []const u8 {
        if (self.isAbsolute()) return self.resolved[1..];
        return self.resolved;
    }
};

pub const Iterator = struct {
    vfs: Vfs,
    dir: Dir,
    ptr: *anyopaque,

    pub const Error = IterateError;

    pub fn next(self: *@This()) !?Dir.Entry {
        const entry = try self.vfs.vtable.iterateNext(self.vfs.ptr, self.dir, self.ptr) orelse return null;
        std.debug.assert(!std.meta.isError(SafePath.validate(entry.basename))); // the vfs impl leaks unsafe paths
        return entry;
    }

    pub fn reset(self: *@This()) void {
        self.vfs.vtable.iterateReset(self.vfs.ptr, self.dir, self.ptr);
    }

    pub fn deinit(self: *@This()) void {
        self.vfs.vtable.iterateDeinit(self.vfs.ptr, self.dir, self.ptr);
        self.* = undefined;
    }

    // used by SelectiveWalker
    fn closeAndDeinit(self: *@This()) void {
        self.vfs.vtable.iterateDeinit(self.vfs.ptr, self.dir, self.ptr);
        self.vfs.closeDir(self.dir);
        self.* = undefined;
    }
};

pub const SelectiveWalker = struct {
    stack: std.ArrayList(StackItem),
    name_buffer: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub const Error = Iterator.Error;

    const StackItem = struct {
        iter: Iterator,
        dirname_len: usize,
    };

    /// After each call to this function, and on deinit(), the memory returned
    /// from this function becomes invalid. A copy must be made in order to keep
    /// a reference to the path.
    pub fn next(self: *SelectiveWalker) Error!?Walker.Entry {
        while (self.stack.items.len > 0) {
            const top = &self.stack.items[self.stack.items.len - 1];
            var dirname_len = top.dirname_len;
            if (top.iter.next() catch |err| {
                // If we get an error, then we want the user to be able to continue
                // walking if they want, which means that we need to pop the directory
                // that errored from the stack. Otherwise, all future `next` calls would
                // likely just fail with the same error.
                self.leave();
                return err;
            }) |entry| {
                self.name_buffer.shrinkRetainingCapacity(dirname_len);
                if (self.name_buffer.items.len != 0) {
                    try self.name_buffer.append(self.allocator, '/');
                    dirname_len += 1;
                }
                try self.name_buffer.ensureUnusedCapacity(self.allocator, entry.basename.len);
                self.name_buffer.appendSliceAssumeCapacity(entry.basename);
                const walker_entry: Walker.Entry = .{
                    .vfs = top.iter.vfs,
                    .dir = top.iter.dir,
                    .stat = entry.stat,
                    .basename = self.name_buffer.items[dirname_len .. self.name_buffer.items.len],
                    .path = self.name_buffer.items[0 .. self.name_buffer.items.len],
                };
                return walker_entry;
            } else {
                self.leave();
            }
        }
        return null;
    }

    /// Traverses into the directory, continuing walking one level down.
    pub fn enter(self: *SelectiveWalker, entry: Walker.Entry) !void {
        if (entry.stat.kind != .dir) {
            @branchHint(.cold);
            return;
        }

        std.debug.assert(!std.meta.isError(SafePath.validate(entry.basename))); // vfs impl leaks invalid paths
        const new_dir = try entry.vfs.openDir(entry.dir, .{ .resolved = entry.basename }, .{ .iterate = true });
        errdefer entry.vfs.closeDir(entry.dir);

        var iter = try entry.vfs.iterate(new_dir);
        errdefer iter.deinit();

        try self.stack.append(self.allocator, .{
            .iter = iter,
            .dirname_len = self.name_buffer.items.len - 1,
        });
    }

    pub fn deinit(self: *SelectiveWalker) void {
        self.name_buffer.deinit(self.allocator);
        while (self.stack.items.len > 0) self.leave();
        self.stack.deinit(self.allocator);
    }

    /// Leaves the current directory, continuing walking one level up.
    /// If the current entry is a directory entry, then the "current directory"
    /// will pertain to that entry if `enter` is called before `leave`.
    pub fn leave(self: *SelectiveWalker) void {
        var item = self.stack.pop().?;
        if (item.dirname_len > 0) {
            @branchHint(.likely);
            item.iter.closeAndDeinit();
        } else {
            item.iter.deinit();
        }
    }
};

pub const Walker = struct {
    inner: SelectiveWalker,

    pub const Entry = struct {
        vfs: Vfs,
        /// The containing directory. This can be used to operate directly on `basename`
        /// rather than `path`, avoiding `error.NameTooLong` for deeply nested paths.
        /// The directory remains open until `next` or `deinit` is called.
        dir: Dir,
        stat: Stat,
        basename: []const u8,
        path: []const u8,

        /// Returns the depth of the entry relative to the initial directory.
        /// Returns 1 for a direct child of the initial directory, 2 for an entry
        /// within a direct child of the initial directory, etc.
        pub fn depth(self: Walker.Entry) usize {
            return std.mem.countScalar(u8, self.path, '/') + 1;
        }
    };

    /// After each call to this function, and on deinit(), the memory returned
    /// from this function becomes invalid. A copy must be made in order to keep
    /// a reference to the path.
    pub fn next(self: *Walker) !?Walker.Entry {
        const entry = try self.inner.next() orelse return null;
        if (entry.stat.kind == .dir) try self.inner.enter(entry);
        return entry;
    }

    pub fn deinit(self: *Walker) void {
        self.inner.deinit();
    }

    /// Leaves the current directory, continuing walking one level up.
    /// If the current entry is a directory entry, then the "current directory"
    /// is the directory pertaining to the current entry.
    pub fn leave(self: *Walker) void {
        self.inner.leave();
    }
};

/// Namespace for unsupported / noop implementations for the VTable
pub const noop = struct {
    pub fn openDir(_: *anyopaque, _: Dir, _: []const u8, _: Dir.OpenOptions) OpenDirError!Dir {
        return error.Unsupported;
    }

    pub fn closeDir(_: *anyopaque, _: Dir) void {}

    pub fn deleteDir(_: *anyopaque, _: Dir, _: []const u8, _: Dir.DeleteOptions) DeleteDirError!void {
        return error.Unsupported;
    }

    pub fn stat(_: *anyopaque, _: Dir, _: []const u8) StatError!Stat {
        return error.Unsupported;
    }

    pub fn iterate(_: *anyopaque, _: Dir) IterateError!*anyopaque {
        return error.Unsupported;
    }

    pub fn iterateNext(_: *anyopaque, _: Dir, _: *anyopaque) IterateError!?Dir.Entry {
        return error.Unsupported;
    }

    pub fn iterateDeinit(_: *anyopaque, _: Dir, _: *anyopaque) void {}

    pub fn openFile(_: *anyopaque, _: Dir, _: []const u8, _: File.OpenOptions) OpenFileError!File {
        return error.Unsupported;
    }

    pub fn closeFile(_: *anyopaque, _: File) void {}

    pub fn deleteFile(_: *anyopaque, _: Dir, _: []const u8) DeleteFileError!void {
        return error.Unsupported;
    }

    pub fn seek(_: *anyopaque, _: File, _: u64, _: File.Whence) SeekError!u64 {
        return error.Unsupported;
    }

    pub fn writev(_: *anyopaque, _: File, _: []const []const u8) WriteError!usize {
        return error.Unsupported;
    }

    pub fn pwritev(_: *anyopaque, _: File, _: []const []const u8, _: u64) WriteError!usize {
        return error.Unsupported;
    }

    pub fn readv(_: *anyopaque, _: File, _: []const []u8) ReadError!usize {
        return error.Unsupported;
    }

    pub fn preadv(_: *anyopaque, _: File, _: []const []u8, _: u64) ReadError!usize {
        return error.Unsupported;
    }
};
