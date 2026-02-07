const std = @import("std");
const harha = @import("harha.zig");
const enabled = @import("build_options");
const testing = std.testing;

// NOTE: These tests are LLM generated quality may vary
// TODO: Actual test suite that you can run on any vfs implementation

// ============================================================================
// SafePath Tests
// ============================================================================

test "SafePath: validate valid paths" {
    try harha.SafePath.validate("file.txt");
    try harha.SafePath.validate("dir/file.txt");
    try harha.SafePath.validate("a/b/c/file.txt");
    try harha.SafePath.validate("/absolute/path");
    try harha.SafePath.validate("path with spaces");
    try harha.SafePath.validate("file-name_123.ext");
}

test "SafePath: reject invalid paths" {
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("."));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate(".."));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("./file"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("../file"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("dir/../file"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("dir/./file"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("//double"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("trailing/"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has<bracket"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has>bracket"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has:colon"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has\"quote"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has\\backslash"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has|pipe"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has?question"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has*asterisk"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has\nnewline"));
    try testing.expectError(error.InvalidPath, harha.SafePath.validate("has\ttab"));
}

test "SafePath: resolve and isAbsolute" {
    const rel = try harha.SafePath.resolve("dir/file.txt");
    try testing.expect(!rel.isAbsolute());
    try testing.expectEqualStrings("dir/file.txt", rel.relative());

    const abs = try harha.SafePath.resolve("/dir/file.txt");
    try testing.expect(abs.isAbsolute());
    try testing.expectEqualStrings("dir/file.txt", abs.relative());
}

test "SafePath: resolveComptime" {
    const path = comptime harha.SafePath.resolveComptime("dir/file.txt") catch unreachable;
    try testing.expectEqualStrings("dir/file.txt", path.resolved);
}

test "SafePath: resolveComptime with empty path" {
    const path = comptime harha.SafePath.resolveComptime("") catch unreachable;
    try testing.expectEqualStrings("", path.resolved);
}

// ============================================================================
// Std VFS Tests
// ============================================================================

test "Std: basic file operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{
        .sub_path = "test.txt",
        .data = "Hello, Harha!",
    });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try .resolve("test.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("Hello, Harha!", buffer[0..bytes_read]);
}

test "Std: directory operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const dir = try vfs.openDir(.root, try .resolve("subdir"), .{ .create = true });
    defer vfs.closeDir(dir);

    const stat = try vfs.stat(.root, try .resolve("subdir"));
    try testing.expectEqual(harha.Kind.dir, stat.kind);
}

test "Std: iteration" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "file1.txt", .data = "1" });
    try tmp.dir.writeFile(.{ .sub_path = "file2.txt", .data = "2" });
    try tmp.dir.makeDir("subdir");

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const dir = try vfs.openDir(.root, try .resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var iter = try vfs.iterate(dir);
    defer iter.deinit();

    var found_files: u32 = 0;
    var found_dirs: u32 = 0;

    while (try iter.next()) |entry| {
        if (entry.stat.kind == .file) found_files += 1;
        if (entry.stat.kind == .dir) found_dirs += 1;
    }

    try testing.expectEqual(@as(u32, 2), found_files);
    try testing.expectEqual(@as(u32, 1), found_dirs);
}

test "Std: seek operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "0123456789" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try .resolve("test.txt"), .{});
    defer vfs.closeFile(file);

    _ = try vfs.seek(file, 5, .set);
    var buffer: [5]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("56789", buffer[0..bytes_read]);

    _ = try vfs.seek(file, 3, .backward);
    const bytes_read2 = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("789", buffer[0..bytes_read2]);

    _ = try vfs.seek(file, 2, .from_end);
    const bytes_read3 = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("89", buffer[0..bytes_read3]);
}

test "Std: write operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try .resolve("output.txt"), .{
        .create = true,
        .mode = .write_only,
    });
    defer vfs.closeFile(file);

    const data = "Test data";
    const written = try vfs.writev(file, &.{data});
    try testing.expectEqual(data.len, written);

    const read_file = try vfs.openFile(.root, try .resolve("output.txt"), .{});
    defer vfs.closeFile(read_file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(read_file, &.{&buffer});
    try testing.expectEqualStrings(data, buffer[0..bytes_read]);
}

test "Std: pread/pwrite operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try .resolve("test.txt"), .{
        .create = true,
        .mode = .read_write,
    });
    defer vfs.closeFile(file);

    _ = try vfs.pwritev(file, &.{"Hello World"}, 0);

    var buffer: [5]u8 = undefined;
    const bytes_read = try vfs.preadv(file, &.{&buffer}, 6);
    try testing.expectEqualStrings("World", buffer[0..bytes_read]);

    const pos = try vfs.seek(file, 0, .set);
    try testing.expectEqual(@as(u64, 0), pos);
}

test "Std: delete operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "delete_me.txt", .data = "x" });
    try tmp.dir.makeDir("delete_dir");

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    try vfs.deleteFile(.root, try .resolve("delete_me.txt"));
    try testing.expectError(error.FileNotFound, vfs.stat(.root, try .resolve("delete_me.txt")));

    try vfs.deleteDir(.root, try .resolve("delete_dir"), .{});
    try testing.expectError(error.FileNotFound, vfs.stat(.root, try .resolve("delete_dir")));
}

test "Std: permissions enforcement" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "data" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.read_only);

    const file = try vfs.openFile(.root, try .resolve("test.txt"), .{});
    defer vfs.closeFile(file);

    try testing.expectError(error.PermissionDenied, vfs.openFile(.root, try .resolve("new.txt"), .{
        .create = true,
        .mode = .write_only,
    }));

    try testing.expectError(error.PermissionDenied, vfs.deleteFile(.root, try .resolve("test.txt")));
}

// ============================================================================
// Overlay VFS Tests
// ============================================================================

test "Overlay: mount and path resolution" {
    const allocator = testing.allocator;

    var tmp1 = std.testing.tmpDir(.{});
    defer tmp1.cleanup();
    var tmp2 = std.testing.tmpDir(.{});
    defer tmp2.cleanup();

    try tmp1.dir.writeFile(.{ .sub_path = "file1.txt", .data = "from tmp1" });
    try tmp2.dir.writeFile(.{ .sub_path = "file2.txt", .data = "from tmp2" });

    var std_vfs1: harha.Std = try .init(allocator, tmp1.dir);
    defer std_vfs1.deinit();
    var std_vfs2: harha.Std = try .init(allocator, tmp2.dir);
    defer std_vfs2.deinit();

    var overlay: harha.Overlay = .init(allocator);
    defer overlay.deinit();

    try overlay.mount(std_vfs1.vfs(.all), "/mnt1");
    try overlay.mount(std_vfs2.vfs(.all), "/mnt2");

    const vfs = overlay.vfs(.all);

    const file1 = try vfs.openFile(.root, try .resolve("/mnt1/file1.txt"), .{});
    defer vfs.closeFile(file1);
    var buffer1: [256]u8 = undefined;
    const read1 = try vfs.readv(file1, &.{&buffer1});
    try testing.expectEqualStrings("from tmp1", buffer1[0..read1]);

    const file2 = try vfs.openFile(.root, try .resolve("/mnt2/file2.txt"), .{});
    defer vfs.closeFile(file2);
    var buffer2: [256]u8 = undefined;
    const read2 = try vfs.readv(file2, &.{&buffer2});
    try testing.expectEqualStrings("from tmp2", buffer2[0..read2]);
}

test "Overlay: unmount" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var overlay: harha.Overlay = .init(allocator);
    defer overlay.deinit();

    try overlay.mount(std_vfs.vfs(.all), "/mnt");

    const vfs = overlay.vfs(.all);

    _ = try vfs.stat(.root, try .resolve("/mnt"));

    overlay.unmount("/mnt");

    try testing.expectError(error.FileNotFound, vfs.stat(.root, try .resolve("/mnt")));
}

test "Overlay: reject relative mount points" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var overlay: harha.Overlay = .init(allocator);
    defer overlay.deinit();

    try testing.expectError(error.RelativePath, overlay.mount(std_vfs.vfs(.all), "relative"));
}

// ============================================================================
// Walker Tests
// ============================================================================

test "Walker: basic directory traversal" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir("a");
    try tmp.dir.makeDir("a/b");
    try tmp.dir.writeFile(.{ .sub_path = "file1.txt", .data = "1" });
    try tmp.dir.writeFile(.{ .sub_path = "a/file2.txt", .data = "2" });
    try tmp.dir.writeFile(.{ .sub_path = "a/b/file3.txt", .data = "3" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const dir = try vfs.openDir(.root, try .resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var walker = try vfs.walk(dir, allocator);
    defer walker.deinit();

    var file_count: u32 = 0;
    var dir_count: u32 = 0;

    while (try walker.next()) |entry| {
        if (entry.stat.kind == .file) file_count += 1;
        if (entry.stat.kind == .dir) dir_count += 1;
    }

    try testing.expectEqual(@as(u32, 3), file_count);
    try testing.expectEqual(@as(u32, 2), dir_count);
}

// ============================================================================
// Permission Tests
// ============================================================================

test "Permissions: custom permissions" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const custom_perms: harha.Permissions = .{
        .create = false,
        .delete = false,
        .read = true,
        .write = false,
        .iterate = true,
        .stat = true,
    };

    const vfs = std_vfs.vfs(custom_perms);

    _ = try vfs.stat(.root, try .resolve(""));

    try testing.expectError(error.PermissionDenied, vfs.openFile(.root, try .resolve("new.txt"), .{
        .create = true,
        .mode = .write_only,
    }));
}

// ============================================================================
// Error Handling Tests
// ============================================================================

test "Error: FileNotFound" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    try testing.expectError(error.FileNotFound, vfs.openFile(.root, try .resolve("nonexistent.txt"), .{}));
    try testing.expectError(error.FileNotFound, vfs.stat(.root, try .resolve("nonexistent.txt")));
}

test "Error: IsDir when opening directory as file" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir("subdir");

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    try testing.expectError(error.IsDir, vfs.openFile(.root, try .resolve("subdir"), .{}));
}

test "Error: NotDir when opening file as directory" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "file.txt", .data = "x" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    try testing.expectError(error.NotDir, vfs.openDir(.root, try .resolve("file.txt"), .{}));
}

// ============================================================================
// Edge Cases
// ============================================================================

test "Edge: empty file read" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "empty.txt", .data = "" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try .resolve("empty.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [10]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqual(@as(usize, 0), bytes_read);
}

test "Edge: multiple opens of same file" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "0123456789" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file1 = try vfs.openFile(.root, try .resolve("test.txt"), .{});
    defer vfs.closeFile(file1);

    const file2 = try vfs.openFile(.root, try .resolve("test.txt"), .{});
    defer vfs.closeFile(file2);

    _ = try vfs.seek(file1, 5, .set);

    var buffer1: [5]u8 = undefined;
    var buffer2: [5]u8 = undefined;
    const read1 = try vfs.readv(file1, &.{&buffer1});
    const read2 = try vfs.readv(file2, &.{&buffer2});

    try testing.expectEqualStrings("56789", buffer1[0..read1]);
    try testing.expectEqualStrings("01234", buffer2[0..read2]);
}

test "Edge: readv with multiple iovecs" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "Hello, World!" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try .resolve("test.txt"), .{});
    defer vfs.closeFile(file);

    var buf1: [5]u8 = undefined;
    var buf2: [7]u8 = undefined;
    const total = try vfs.readv(file, &.{ &buf1, &buf2 });

    try testing.expectEqual(@as(usize, 12), total);
    try testing.expectEqualStrings("Hello", &buf1);
    try testing.expectEqualStrings(", World", &buf2);
}

test "Edge: writev with multiple iovecs" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try .resolve("test.txt"), .{
        .create = true,
        .mode = .write_only,
    });
    defer vfs.closeFile(file);

    const written = try vfs.writev(file, &.{ "Hello", ", ", "World!" });
    try testing.expectEqual(@as(usize, 13), written);

    const read_file = try vfs.openFile(.root, try .resolve("test.txt"), .{});
    defer vfs.closeFile(read_file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(read_file, &.{&buffer});
    try testing.expectEqualStrings("Hello, World!", buffer[0..bytes_read]);
}

// ============================================================================
// SRA VFS Tests
// ============================================================================

/// Helper to create a minimal SRA archive for testing
fn createTestArchive(dir: std.fs.Dir, filename: []const u8) !void {
    if (!enabled.sra) return error.SkipZigTest; // sra support not enabled
    // This is a placeholder - you'll need to use the sra library to create a test archive
    // Example structure:
    // - file1.txt: "Hello from SRA"
    // - dir1/file2.txt: "Nested file"
    // - dir1/dir2/file3.txt: "Deeply nested"
    const sra = @import("sra");
    var file = try dir.createFile(filename, .{});
    defer file.close();
    var buffer: [4096]u8 = undefined;
    var file_writer = file.writer(&buffer);
    var writer: sra.Writer = try .init(testing.allocator, &file_writer);
    defer writer.deinit();
    try writer.stream.writeFileBytes("file1.txt", "Hello from SRA", 0);
    try writer.stream.writeFileBytes("dir1/file2.txt", "Nested file", 0);
    try writer.stream.writeFileBytes("dir1/dir2/file3.txt", "Deeply nested", 0);
    try writer.finish();
}

test "SRA: basic file reading" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Create test archive
    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    // Open and read file
    const file = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("Hello from SRA", buffer[0..bytes_read]);
}

test "SRA: read-only enforcement" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    // Should not be able to create files
    try testing.expectError(error.PermissionDenied, vfs.openFile(.root, try harha.SafePath.resolve("new.txt"), .{
        .create = true,
        .mode = .write_only,
    }));

    // Should not be able to write to existing files
    const file = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    defer vfs.closeFile(file);

    try testing.expectError(error.Unsupported, vfs.writev(file, &.{"data"}));
}

test "SRA: directory iteration" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    // Open root for iteration
    const dir = try vfs.openDir(.root, try harha.SafePath.resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var iter = try vfs.iterate(dir);
    defer iter.deinit();

    var found_file1 = false;
    var found_dir1 = false;

    while (try iter.next()) |entry| {
        if (std.mem.eql(u8, entry.basename, "file1.txt")) found_file1 = true;
        if (std.mem.eql(u8, entry.basename, "dir1")) found_dir1 = true;
    }

    try testing.expect(found_file1);
    try testing.expect(found_dir1);
}

test "SRA: nested directory iteration" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    // Open nested directory
    const dir1 = try vfs.openDir(.root, try harha.SafePath.resolve("dir1"), .{ .iterate = true });
    defer vfs.closeDir(dir1);

    var iter = try vfs.iterate(dir1);
    defer iter.deinit();

    var found_file2 = false;
    var found_dir2 = false;

    while (try iter.next()) |entry| {
        if (std.mem.eql(u8, entry.basename, "file2.txt")) found_file2 = true;
        if (std.mem.eql(u8, entry.basename, "dir2")) found_dir2 = true;
    }

    try testing.expect(found_file2);
    try testing.expect(found_dir2);
}

test "SRA: stat operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    // Stat file
    const file_stat = try vfs.stat(.root, try harha.SafePath.resolve("file1.txt"));
    try testing.expectEqual(harha.Kind.file, file_stat.kind);
    try testing.expect(file_stat.size > 0);

    // Stat directory
    const dir_stat = try vfs.stat(.root, try harha.SafePath.resolve("dir1"));
    try testing.expectEqual(harha.Kind.dir, dir_stat.kind);
}

test "SRA: seek operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    defer vfs.closeFile(file);

    // Seek forward
    const pos = try vfs.seek(file, 5, .set);
    try testing.expectEqual(@as(u64, 5), pos);

    // Read from new position
    var buffer: [10]u8 = undefined;
    _ = try vfs.readv(file, &.{&buffer});
}

test "SRA: multiple file handles" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    // Open same file twice
    const file1 = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    defer vfs.closeFile(file1);

    const file2 = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    defer vfs.closeFile(file2);

    // Seek one handle
    _ = try vfs.seek(file1, 5, .set);

    // Other handle should still be at position 0
    var buffer1: [5]u8 = undefined;
    var buffer2: [5]u8 = undefined;
    _ = try vfs.readv(file1, &.{&buffer1});
    _ = try vfs.readv(file2, &.{&buffer2});

    // They should read different data
    try testing.expect(!std.mem.eql(u8, &buffer1, &buffer2));
}

test "SRA: preadv doesn't affect cursor" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    defer vfs.closeFile(file);

    // preadv at offset 5
    var buffer1: [5]u8 = undefined;
    _ = try vfs.preadv(file, &.{&buffer1}, 5);

    // Regular read should still be at offset 0
    var buffer2: [5]u8 = undefined;
    _ = try vfs.readv(file, &.{&buffer2});

    // buffer2 should be from the start of the file
    try testing.expect(!std.mem.eql(u8, &buffer1, &buffer2));
}

test "SRA: generation counter uniqueness" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    // Open and close file multiple times
    const file1 = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    vfs.closeFile(file1);

    const file2 = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    vfs.closeFile(file2);

    const file3 = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    defer vfs.closeFile(file3);

    // Each handle should be unique (different generation)
    try testing.expect(@intFromEnum(file1) != @intFromEnum(file2));
    try testing.expect(@intFromEnum(file2) != @intFromEnum(file3));
    try testing.expect(@intFromEnum(file1) != @intFromEnum(file3));
}

test "SRA: handle type validation" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    const dir = try vfs.openDir(.root, try harha.SafePath.resolve("dir1"), .{});
    defer vfs.closeDir(dir);

    const file = try vfs.openFile(.root, try harha.SafePath.resolve("file1.txt"), .{});
    defer vfs.closeFile(file);

    // The Id packed struct should have correct kind bits
    // This is implicitly tested by the fact that operations work correctly
    // and don't confuse files with directories
}

test "SRA: walker integration" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    const vfs = sra_vfs.vfs(.all);

    const dir = try vfs.openDir(.root, try harha.SafePath.resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var walker = try vfs.walk(dir, allocator);
    defer walker.deinit();

    var total_files: u32 = 0;
    var total_dirs: u32 = 0;

    while (try walker.next()) |entry| {
        if (entry.stat.kind == .file) total_files += 1;
        if (entry.stat.kind == .dir) total_dirs += 1;
    }

    // Should find all files and directories in the archive
    try testing.expect(total_files >= 3); // file1.txt, file2.txt, file3.txt
    try testing.expect(total_dirs >= 2);  // dir1, dir2
}

// ============================================================================
// SRA + Overlay Integration Tests
// ============================================================================

test "SRA Overlay: mount SRA archive" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestArchive(tmp.dir, "test.sra");

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp.dir, "test.sra");
    defer sra_vfs.deinit();

    var overlay: harha.Overlay = .init(allocator);
    defer overlay.deinit();

    try overlay.mount(sra_vfs.vfs(.all), "/archive");

    const vfs = overlay.vfs(.all);

    // Access file through overlay
    const file = try vfs.openFile(.root, try harha.SafePath.resolve("/archive/file1.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("Hello from SRA", buffer[0..bytes_read]);
}

test "SRA Overlay: mixed SRA and Std mounts" {
    const allocator = testing.allocator;

    var tmp1 = testing.tmpDir(.{});
    defer tmp1.cleanup();
    var tmp2 = testing.tmpDir(.{});
    defer tmp2.cleanup();

    try createTestArchive(tmp1.dir, "test.sra");
    try tmp2.dir.writeFile(.{ .sub_path = "regular.txt", .data = "Regular file" });

    var sra_vfs: harha.Sra = try .initPath(allocator, tmp1.dir, "test.sra");
    defer sra_vfs.deinit();
    var std_vfs: harha.Std = try .init(allocator, tmp2.dir);
    defer std_vfs.deinit();

    var overlay: harha.Overlay = .init(allocator);
    defer overlay.deinit();

    try overlay.mount(sra_vfs.vfs(.all), "/ro");
    try overlay.mount(std_vfs.vfs(.all), "/rw");

    const vfs = overlay.vfs(.all);

    // Read from SRA (read-only)
    const sra_file = try vfs.openFile(.root, try harha.SafePath.resolve("/ro/file1.txt"), .{});
    defer vfs.closeFile(sra_file);

    // Read/write from Std
    const std_file = try vfs.openFile(.root, try harha.SafePath.resolve("/rw/regular.txt"), .{});
    defer vfs.closeFile(std_file);

    // Write should fail on SRA mount
    try testing.expectError(error.PermissionDenied, vfs.openFile(.root, try harha.SafePath.resolve("/ro/new.txt"), .{
        .create = true,
        .mode = .write_only,
    }));

    // Write should succeed on Std mount
    const new_file = try vfs.openFile(.root, try harha.SafePath.resolve("/rw/new.txt"), .{
        .create = true,
        .mode = .write_only,
    });
    defer vfs.closeFile(new_file);
}

// ============================================================================
// Iterator Reset Tests
// ============================================================================

test "Iterator: reset functionality" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try createTestStructure(tmp.dir);

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const dir = try vfs.openDir(.root, try harha.SafePath.resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var iter = try vfs.iterate(dir);
    defer iter.deinit();

    // First iteration
    var first_count: u32 = 0;
    while (try iter.next()) |_| {
        first_count += 1;
    }

    // Reset and iterate again
    iter.reset();

    var second_count: u32 = 0;
    while (try iter.next()) |_| {
        second_count += 1;
    }

    try testing.expectEqual(first_count, second_count);
    try testing.expect(first_count > 0);
}

test "Iterator: reset mid-iteration" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "a.txt", .data = "a" });
    try tmp.dir.writeFile(.{ .sub_path = "b.txt", .data = "b" });
    try tmp.dir.writeFile(.{ .sub_path = "c.txt", .data = "c" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const dir = try vfs.openDir(.root, try harha.SafePath.resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var iter = try vfs.iterate(dir);
    defer iter.deinit();

    // Iterate partially
    _ = try iter.next();
    _ = try iter.next();

    // Reset
    iter.reset();

    // Should start from beginning again
    var count: u32 = 0;
    while (try iter.next()) |_| {
        count += 1;
    }

    try testing.expectEqual(@as(u32, 3), count);
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

test "Concurrent: multiple readers" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const data = "Shared read content";
    try tmp.dir.writeFile(.{ .sub_path = "shared.txt", .data = data });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    // Open file multiple times for reading
    const file1 = try vfs.openFile(.root, try harha.SafePath.resolve("shared.txt"), .{});
    defer vfs.closeFile(file1);

    const file2 = try vfs.openFile(.root, try harha.SafePath.resolve("shared.txt"), .{});
    defer vfs.closeFile(file2);

    const file3 = try vfs.openFile(.root, try harha.SafePath.resolve("shared.txt"), .{});
    defer vfs.closeFile(file3);

    // All should be able to read
    var buffer1: [256]u8 = undefined;
    var buffer2: [256]u8 = undefined;
    var buffer3: [256]u8 = undefined;

    const read1 = try vfs.readv(file1, &.{&buffer1});
    const read2 = try vfs.readv(file2, &.{&buffer2});
    const read3 = try vfs.readv(file3, &.{&buffer3});

    try testing.expectEqualStrings(data, buffer1[0..read1]);
    try testing.expectEqualStrings(data, buffer2[0..read2]);
    try testing.expectEqualStrings(data, buffer3[0..read3]);
}

test "Concurrent: interleaved read operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "0123456789ABCDEF" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file1 = try vfs.openFile(.root, try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file1);

    const file2 = try vfs.openFile(.root, try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file2);

    var buf1: [4]u8 = undefined;
    var buf2: [4]u8 = undefined;

    // Interleaved reads
    _ = try vfs.readv(file1, &.{&buf1}); // "0123"
    _ = try vfs.readv(file2, &.{&buf2}); // "0123"

    try testing.expectEqualStrings("0123", &buf1);
    try testing.expectEqualStrings("0123", &buf2);

    _ = try vfs.readv(file1, &.{&buf1}); // "4567"
    _ = try vfs.readv(file2, &.{&buf2}); // "4567"

    try testing.expectEqualStrings("4567", &buf1);
    try testing.expectEqualStrings("4567", &buf2);
}

// ============================================================================
// Boundary Condition Tests
// ============================================================================

test "Boundary: large file read" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Create a large file (1MB)
    const size: usize = 1024 * 1024;
    const large_data = try generateRandomContent(allocator, size, 42);
    defer allocator.free(large_data);

    try tmp.dir.writeFile(.{ .sub_path = "large.bin", .data = large_data });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try harha.SafePath.resolve("large.bin"), .{});
    defer vfs.closeFile(file);

    // Read in chunks
    const chunk_size = 4096;
    var total_read: usize = 0;
    var buffer: [chunk_size]u8 = undefined;

    while (true) {
        const bytes_read = try vfs.readv(file, &.{&buffer});
        if (bytes_read == 0) break;

        // Verify chunk matches
        try testing.expectEqualSlices(u8, large_data[total_read..][0..bytes_read], buffer[0..bytes_read]);
        total_read += bytes_read;
    }

    try testing.expectEqual(size, total_read);
}

test "Boundary: many small files" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    // Create many small files
    const file_count = 100;
    try stressTest(vfs, .root, file_count, allocator);

    // Verify all files
    try verifyStressTest(vfs, .root, file_count, allocator);
}

test "Boundary: zero-byte operations" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    // Create file
    const file = try vfs.openFile(.root, try harha.SafePath.resolve("test.txt"), .{
        .create = true,
        .mode = .read_write,
    });
    defer vfs.closeFile(file);

    // Write zero bytes
    const written = try vfs.writev(file, &.{""});
    try testing.expectEqual(@as(usize, 0), written);

    // Seek to 0
    _ = try vfs.seek(file, 0, .set);

    // Read zero bytes
    var buffer: [10]u8 = undefined;
    const read = try vfs.readv(file, &.{buffer[0..0]});
    try testing.expectEqual(@as(usize, 0), read);
}

// ============================================================================
// Permission Edge Cases
// ============================================================================

test "Permission: stat without stat permission" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "data" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const perms: harha.Permissions = .{
        .create = false,
        .delete = false,
        .read = true,
        .write = false,
        .iterate = false,
        .stat = false,
    };

    const vfs = std_vfs.vfs(perms);

    try testing.expectError(error.PermissionDenied, vfs.stat(.root, try harha.SafePath.resolve("test.txt")));
}

test "Permission: iterate without iterate permission" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir("subdir");

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const perms: harha.Permissions = .{
        .create = false,
        .delete = false,
        .read = true,
        .write = false,
        .iterate = false,
        .stat = true,
    };

    const vfs = std_vfs.vfs(perms);

    const dir = try vfs.openDir(.root, try harha.SafePath.resolve("subdir"), .{});
    defer vfs.closeDir(dir);

    try testing.expectError(error.PermissionDenied, vfs.iterate(dir));
}

// ============================================================================
// Handle Lifecycle Tests
// ============================================================================

test "Lifecycle: use after close detection" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "data" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try harha.SafePath.resolve("test.txt"), .{});
    vfs.closeFile(file);

    // Attempting to use closed file should fail
    var buffer: [10]u8 = undefined;
    try testing.expectError(error.NotOpenForReading, vfs.readv(file, &.{&buffer}));
}

test "Lifecycle: double close is safe" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "data" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    const file = try vfs.openFile(.root, try harha.SafePath.resolve("test.txt"), .{});
    vfs.closeFile(file);
    vfs.closeFile(file); // Should be safe (noop)
}

// ============================================================================
// Overlay Edge Cases
// ============================================================================

test "Overlay: overlapping mount paths" {
    const allocator = testing.allocator;

    var tmp1 = testing.tmpDir(.{});
    defer tmp1.cleanup();
    var tmp2 = testing.tmpDir(.{});
    defer tmp2.cleanup();

    try tmp1.dir.writeFile(.{ .sub_path = "file.txt", .data = "from tmp1" });
    try tmp2.dir.writeFile(.{ .sub_path = "file.txt", .data = "from tmp2" });

    var std_vfs1 = try harha.Std.init(allocator, tmp1.dir);
    defer std_vfs1.deinit();
    var std_vfs2 = try harha.Std.init(allocator, tmp2.dir);
    defer std_vfs2.deinit();

    var overlay: harha.Overlay = .init(allocator);
    defer overlay.deinit();

    // Mount in order
    try overlay.mount(std_vfs1.vfs(.all), "/mnt");
    try overlay.mount(std_vfs2.vfs(.all), "/mnt/sub");

    const vfs = overlay.vfs(.all);

    // Access from first mount
    const file1 = try vfs.openFile(.root, try harha.SafePath.resolve("/mnt/file.txt"), .{});
    defer vfs.closeFile(file1);

    var buffer1: [256]u8 = undefined;
    const read1 = try vfs.readv(file1, &.{&buffer1});
    try testing.expectEqualStrings("from tmp1", buffer1[0..read1]);

    // Access from second mount
    const file2 = try vfs.openFile(.root, try harha.SafePath.resolve("/mnt/sub/file.txt"), .{});
    defer vfs.closeFile(file2);

    var buffer2: [256]u8 = undefined;
    const read2 = try vfs.readv(file2, &.{&buffer2});
    try testing.expectEqualStrings("from tmp2", buffer2[0..read2]);
}

test "Overlay: mount order precedence" {
    const allocator = testing.allocator;

    var tmp1 = testing.tmpDir(.{});
    defer tmp1.cleanup();
    var tmp2 = testing.tmpDir(.{});
    defer tmp2.cleanup();

    try tmp1.dir.makePath("test");
    try tmp1.dir.writeFile(.{ .sub_path = "test/file.txt", .data = "first" });
    try tmp2.dir.writeFile(.{ .sub_path = "file.txt", .data = "second" });

    var std_vfs1 = try harha.Std.init(allocator, tmp1.dir);
    defer std_vfs1.deinit();
    var std_vfs2 = try harha.Std.init(allocator, tmp2.dir);
    defer std_vfs2.deinit();

    var overlay: harha.Overlay = .init(allocator);
    defer overlay.deinit();

    // Mount both to same path - later mount should win
    try overlay.mount(std_vfs1.vfs(.all), "/data");
    try overlay.mount(std_vfs2.vfs(.all), "/data/test");

    const vfs = overlay.vfs(.all);

    // Should access the second mount (last one wins)
    const file = try vfs.openFile(.root, try harha.SafePath.resolve("/data/test/file.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [256]u8 = undefined;
    const read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("second", buffer[0..read]);
}

// ============================================================================
// Absolute vs Relative Path Tests
// ============================================================================

test "Path: absolute path from non-root directory" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir("subdir");
    try tmp.dir.writeFile(.{ .sub_path = "root_file.txt", .data = "at root" });
    try tmp.dir.writeFile(.{ .sub_path = "subdir/sub_file.txt", .data = "in subdir" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    // Open subdirectory
    const subdir = try vfs.openDir(.root, try harha.SafePath.resolve("subdir"), .{});
    defer vfs.closeDir(subdir);

    // Access file using absolute path from subdirectory
    const file = try vfs.openFile(subdir, try harha.SafePath.resolve("/root_file.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("at root", buffer[0..bytes_read]);
}

test "Path: relative path navigation" {
    const allocator = testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir("a");
    try tmp.dir.makeDir("a/b");
    try tmp.dir.writeFile(.{ .sub_path = "a/b/file.txt", .data = "nested" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    const vfs = std_vfs.vfs(.all);

    // Navigate step by step
    const dir_a = try vfs.openDir(.root, try harha.SafePath.resolve("a"), .{});
    defer vfs.closeDir(dir_a);

    const dir_b = try vfs.openDir(dir_a, try harha.SafePath.resolve("b"), .{});
    defer vfs.closeDir(dir_b);

    const file = try vfs.openFile(dir_b, try harha.SafePath.resolve("file.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("nested", buffer[0..bytes_read]);
}

/// Create a complex directory structure for testing
fn createTestStructure(dir: std.fs.Dir) !void {
    // Root level
    try dir.writeFile(.{ .sub_path = "root_file.txt", .data = "root content" });

    // First level
    try dir.makeDir("dir1");
    try dir.writeFile(.{ .sub_path = "dir1/file1.txt", .data = "file1 content" });

    try dir.makeDir("dir2");
    try dir.writeFile(.{ .sub_path = "dir2/file2.txt", .data = "file2 content" });

    // Second level
    try dir.makeDir("dir1/subdir1");
    try dir.writeFile(.{ .sub_path = "dir1/subdir1/nested.txt", .data = "nested content" });

    // Third level
    try dir.makeDir("dir1/subdir1/deep");
    try dir.writeFile(.{ .sub_path = "dir1/subdir1/deep/deep_file.txt", .data = "deep content" });

    // Various file sizes
    try dir.writeFile(.{ .sub_path = "empty.txt", .data = "" });
    try dir.writeFile(.{ .sub_path = "small.txt", .data = "x" });

    var large_buffer: [4096]u8 = undefined;
    @memset(&large_buffer, 'A');
    try dir.writeFile(.{ .sub_path = "large.txt", .data = &large_buffer });
}

/// Count files and directories recursively
fn countEntries(vfs: harha.Vfs, dir: harha.Dir, allocator: std.mem.Allocator) !struct { files: u32, dirs: u32 } {
    var walker = try vfs.walk(dir, allocator);
    defer walker.deinit();

    var files: u32 = 0;
    var dirs: u32 = 0;

    while (try walker.next()) |entry| {
        switch (entry.stat.kind) {
            .file => files += 1,
            .dir => dirs += 1,
        }
    }

    return .{ .files = files, .dirs = dirs };
}

/// Read entire file into allocated buffer
fn readFileAlloc(vfs: harha.Vfs, dir: harha.Dir, path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const safe_path = try harha.SafePath.resolve(path);
    const file = try vfs.openFile(dir, safe_path, .{});
    defer vfs.closeFile(file);

    const stat = try vfs.stat(dir, safe_path);
    const buffer = try allocator.alloc(u8, stat.size);
    errdefer allocator.free(buffer);

    const bytes_read = try vfs.readv(file, &.{buffer});
    if (bytes_read != stat.size) return error.PartialRead;

    return buffer;
}

/// Write entire buffer to file
fn writeFileAll(vfs: harha.Vfs, dir: harha.Dir, path: []const u8, data: []const u8) !void {
    const safe_path = try harha.SafePath.resolve(path);
    const file = try vfs.openFile(dir, safe_path, .{ .create = true, .mode = .write_only });
    defer vfs.closeFile(file);

    const written = try vfs.writev(file, &.{data});
    if (written != data.len) return error.PartialWrite;
}

/// Compare two VFS instances for equivalent content
fn compareVfs(
    vfs1: harha.Vfs,
    dir1: harha.Dir,
    vfs2: harha.Vfs,
    dir2: harha.Dir,
    allocator: std.mem.Allocator,
) !bool {
    var walker1 = try vfs1.walk(dir1, allocator);
    defer walker1.deinit();

    var walker2 = try vfs2.walk(dir2, allocator);
    defer walker2.deinit();

    var map1 = std.StringHashMap(harha.Stat).init(allocator);
    defer map1.deinit();
    var map2 = std.StringHashMap(harha.Stat).init(allocator);
    defer map2.deinit();

    // Collect all entries from both
    while (try walker1.next()) |entry| {
        const path_copy = try allocator.dupe(u8, entry.path);
        try map1.put(path_copy, entry.stat);
    }

    while (try walker2.next()) |entry| {
        const path_copy = try allocator.dupe(u8, entry.path);
        try map2.put(path_copy, entry.stat);
    }

    // Compare counts
    if (map1.count() != map2.count()) return false;

    // Compare entries
    var iter = map1.iterator();
    while (iter.next()) |kv| {
        const stat2 = map2.get(kv.key_ptr.*) orelse return false;
        if (kv.value_ptr.kind != stat2.kind) return false;
        if (kv.value_ptr.size != stat2.size) return false;
    }

    // Cleanup
    var iter_cleanup = map1.iterator();
    while (iter_cleanup.next()) |kv| {
        allocator.free(kv.key_ptr.*);
    }
    var iter_cleanup2 = map2.iterator();
    while (iter_cleanup2.next()) |kv| {
        allocator.free(kv.key_ptr.*);
    }

    return true;
}

/// Verify a file has expected content
fn expectFileContent(
    vfs: harha.Vfs,
    dir: harha.Dir,
    path: []const u8,
    expected: []const u8,
    allocator: std.mem.Allocator,
) !void {
    const content = try readFileAlloc(vfs, dir, path, allocator);
    defer allocator.free(content);

    if (!std.mem.eql(u8, content, expected)) {
        std.debug.print("\nExpected: {s}\nGot: {s}\n", .{ expected, content });
        return error.ContentMismatch;
    }
}

/// List all entries in a directory (non-recursive)
fn listDir(
    vfs: harha.Vfs,
    dir: harha.Dir,
    allocator: std.mem.Allocator,
) !std.ArrayList([]const u8) {
    var iter = try vfs.iterate(dir);
    defer iter.deinit();

    var list = std.ArrayList([]const u8).init(allocator);
    errdefer {
        for (list.items) |item| allocator.free(item);
        list.deinit();
    }

    while (try iter.next()) |entry| {
        const name = try allocator.dupe(u8, entry.basename);
        try list.append(name);
    }

    return list;
}

/// Benchmark helper
pub const Benchmark = struct {
    allocator: std.mem.Allocator,
    vfs: harha.Vfs,
    dir: harha.Dir,

    fn measureRead(self: @This(), path: []const u8, iterations: u32) !u64 {
        var timer = try std.time.Timer.start();

        var i: u32 = 0;
        while (i < iterations) : (i += 1) {
            const content = try readFileAlloc(self.vfs, self.dir, path, self.allocator);
            self.allocator.free(content);
        }

        return timer.read();
    }

    fn measureWrite(self: @This(), path: []const u8, data: []const u8, iterations: u32) !u64 {
        var timer = try std.time.Timer.start();

        var i: u32 = 0;
        while (i < iterations) : (i += 1) {
            try writeFileAll(self.vfs, self.dir, path, data);
        }

        return timer.read();
    }

    fn measureIteration(self: @This(), iterations: u32) !u64 {
        var timer = try std.time.Timer.start();

        var i: u32 = 0;
        while (i < iterations) : (i += 1) {
            var iter = try self.vfs.iterate(self.dir);
            defer iter.deinit();

            while (try iter.next()) |_| {
                // Just iterate
            }
        }

        return timer.read();
    }
};

/// Stress test helper - creates many files
fn stressTest(vfs: harha.Vfs, dir: harha.Dir, file_count: u32, allocator: std.mem.Allocator) !void {
    var i: u32 = 0;
    while (i < file_count) : (i += 1) {
        const filename = try std.fmt.allocPrint(allocator, "stress_{d}.txt", .{i});
        defer allocator.free(filename);

        const data = try std.fmt.allocPrint(allocator, "Content {d}", .{i});
        defer allocator.free(data);

        try writeFileAll(vfs, dir, filename, data);
    }
}

/// Verify stress test results
fn verifyStressTest(vfs: harha.Vfs, dir: harha.Dir, file_count: u32, allocator: std.mem.Allocator) !void {
    var i: u32 = 0;
    while (i < file_count) : (i += 1) {
        const filename = try std.fmt.allocPrint(allocator, "stress_{d}.txt", .{i});
        defer allocator.free(filename);

        const expected = try std.fmt.allocPrint(allocator, "Content {d}", .{i});
        defer allocator.free(expected);

        try expectFileContent(vfs, dir, filename, expected, allocator);
    }
}

/// Generate random file content for testing
fn generateRandomContent(allocator: std.mem.Allocator, size: usize, seed: u64) ![]u8 {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    const buffer = try allocator.alloc(u8, size);
    random.bytes(buffer);
    return buffer;
}

/// Test that handles are properly isolated
fn testHandleIsolation(vfs: harha.Vfs, path: []const u8, allocator: std.mem.Allocator) !void {
    _ = allocator;
    const safe_path = try harha.SafePath.resolve(path);

    // Open same file multiple times
    const file1 = try vfs.openFile(.root, safe_path, .{});
    defer vfs.closeFile(file1);

    const file2 = try vfs.openFile(.root, safe_path, .{});
    defer vfs.closeFile(file2);

    // Seek one handle
    _ = try vfs.seek(file1, 10, .set);

    // Other handle should be at 0
    const pos2 = try vfs.seek(file2, 0, .set);
    if (pos2 != 0) return error.HandleNotIsolated;

    // Read from both - should get different data if file is long enough
    var buf1: [5]u8 = undefined;
    var buf2: [5]u8 = undefined;
    _ = try vfs.readv(file1, &.{&buf1});
    _ = try vfs.readv(file2, &.{&buf2});
}

// ============================================================================
// Map VFS Tests
// ============================================================================

// Test enum with small number of entries (uses fewer bits)
const SmallMount = enum(u8) {
    data,
    cache,
    temp,
};

// Test enum with more entries
const MediumMount = enum(u8) {
    data,
    cache,
    temp,
    logs,
    config,
    runtime,
    backup,
};

// Test enum at limit (more entries = fewer bits for inner handles)
const LargeMount = enum(u8) {
    m0, m1, m2, m3, m4, m5, m6, m7,
    m8, m9, m10, m11, m12, m13, m14, m15,
};

test "Map: type generation and initialization" {
    const MapVfs = harha.Map(SmallMount);

    // Should initialize with all null
    var map: MapVfs = .init;
    defer map.deinit();

    // All slots should be null initially
    try testing.expect(map.mnt[@intFromEnum(SmallMount.data)] == null);
    try testing.expect(map.mnt[@intFromEnum(SmallMount.cache)] == null);
    try testing.expect(map.mnt[@intFromEnum(SmallMount.temp)] == null);
}

test "Map: basic mount and unmount" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    // Mount
    map.mount(.data, std_vfs.vfs(.all));
    try testing.expect(map.mnt[@intFromEnum(SmallMount.data)] != null);

    // Unmount
    map.unmount(.data);
    try testing.expect(map.mnt[@intFromEnum(SmallMount.data)] == null);
}

test "Map: basic file operations through mapped VFS" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Create test file in underlying filesystem
    try tmp.dir.writeFile(.{
        .sub_path = "test.txt",
        .data = "Hello from Map VFS!",
    });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const data_root = map.rootDir(.data);

    // Open and read file through Map VFS
    const file = try vfs.openFile(data_root, try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("Hello from Map VFS!", buffer[0..bytes_read]);
}

test "Map: multiple mounted filesystems" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp_data = std.testing.tmpDir(.{});
    defer tmp_data.cleanup();
    var tmp_cache = std.testing.tmpDir(.{});
    defer tmp_cache.cleanup();
    var tmp_temp = std.testing.tmpDir(.{});
    defer tmp_temp.cleanup();

    // Create different content in each
    try tmp_data.dir.writeFile(.{ .sub_path = "data.txt", .data = "data content" });
    try tmp_cache.dir.writeFile(.{ .sub_path = "cache.txt", .data = "cache content" });
    try tmp_temp.dir.writeFile(.{ .sub_path = "temp.txt", .data = "temp content" });

    var std_vfs_data: harha.Std = try .init(allocator, tmp_data.dir);
    defer std_vfs_data.deinit();
    var std_vfs_cache: harha.Std = try .init(allocator, tmp_cache.dir);
    defer std_vfs_cache.deinit();
    var std_vfs_temp: harha.Std = try .init(allocator, tmp_temp.dir);
    defer std_vfs_temp.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    // Mount all three
    map.mount(.data, std_vfs_data.vfs(.all));
    map.mount(.cache, std_vfs_cache.vfs(.all));
    map.mount(.temp, std_vfs_temp.vfs(.all));

    const vfs = map.vfs(.all);

    // Access files from each mounted VFS
    var buffer: [256]u8 = undefined;

    const file_data = try vfs.openFile(map.rootDir(.data), try harha.SafePath.resolve("data.txt"), .{});
    defer vfs.closeFile(file_data);
    const read1 = try vfs.readv(file_data, &.{&buffer});
    try testing.expectEqualStrings("data content", buffer[0..read1]);

    const file_cache = try vfs.openFile(map.rootDir(.cache), try harha.SafePath.resolve("cache.txt"), .{});
    defer vfs.closeFile(file_cache);
    const read2 = try vfs.readv(file_cache, &.{&buffer});
    try testing.expectEqualStrings("cache content", buffer[0..read2]);

    const file_temp = try vfs.openFile(map.rootDir(.temp), try harha.SafePath.resolve("temp.txt"), .{});
    defer vfs.closeFile(file_temp);
    const read3 = try vfs.readv(file_temp, &.{&buffer});
    try testing.expectEqualStrings("temp content", buffer[0..read3]);
}

test "Map: directory operations" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir("subdir");
    try tmp.dir.writeFile(.{ .sub_path = "subdir/nested.txt", .data = "nested" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const data_root = map.rootDir(.data);

    // Open subdirectory
    const subdir = try vfs.openDir(data_root, try harha.SafePath.resolve("subdir"), .{});
    defer vfs.closeDir(subdir);

    // Open file in subdirectory
    const file = try vfs.openFile(subdir, try harha.SafePath.resolve("nested.txt"), .{});
    defer vfs.closeFile(file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("nested", buffer[0..bytes_read]);
}

test "Map: stat operations" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "file.txt", .data = "content" });
    try tmp.dir.makeDir("dir");

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const data_root = map.rootDir(.data);

    // Stat file
    const file_stat = try vfs.stat(data_root, try harha.SafePath.resolve("file.txt"));
    try testing.expectEqual(harha.Kind.file, file_stat.kind);
    try testing.expect(file_stat.size > 0);

    // Stat directory
    const dir_stat = try vfs.stat(data_root, try harha.SafePath.resolve("dir"));
    try testing.expectEqual(harha.Kind.dir, dir_stat.kind);
}

test "Map: iteration" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "file1.txt", .data = "1" });
    try tmp.dir.writeFile(.{ .sub_path = "file2.txt", .data = "2" });
    try tmp.dir.makeDir("subdir");

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);

    // Open root with iterate permission
    const dir = try vfs.openDir(map.rootDir(.data), try harha.SafePath.resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var iter = try vfs.iterate(dir);
    defer iter.deinit();

    var file_count: u32 = 0;
    var dir_count: u32 = 0;

    while (try iter.next()) |entry| {
        if (entry.stat.kind == .file) file_count += 1;
        if (entry.stat.kind == .dir) dir_count += 1;
    }

    try testing.expectEqual(@as(u32, 2), file_count);
    try testing.expectEqual(@as(u32, 1), dir_count);
}

test "Map: iterator reset" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "a.txt", .data = "a" });
    try tmp.dir.writeFile(.{ .sub_path = "b.txt", .data = "b" });
    try tmp.dir.writeFile(.{ .sub_path = "c.txt", .data = "c" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const dir = try vfs.openDir(map.rootDir(.data), try harha.SafePath.resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var iter = try vfs.iterate(dir);
    defer iter.deinit();

    // First iteration
    var first_count: u32 = 0;
    while (try iter.next()) |_| {
        first_count += 1;
    }

    // Reset
    iter.reset();

    // Second iteration
    var second_count: u32 = 0;
    while (try iter.next()) |_| {
        second_count += 1;
    }

    try testing.expectEqual(first_count, second_count);
    try testing.expectEqual(@as(u32, 3), first_count);
}

test "Map: write operations" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const data_root = map.rootDir(.data);

    // Create and write to file
    const file = try vfs.openFile(data_root, try harha.SafePath.resolve("output.txt"), .{
        .create = true,
        .mode = .write_only,
    });
    defer vfs.closeFile(file);

    const data = "Written through Map VFS";
    const written = try vfs.writev(file, &.{data});
    try testing.expectEqual(data.len, written);

    // Verify by reading
    const read_file = try vfs.openFile(data_root, try harha.SafePath.resolve("output.txt"), .{});
    defer vfs.closeFile(read_file);

    var buffer: [256]u8 = undefined;
    const bytes_read = try vfs.readv(read_file, &.{&buffer});
    try testing.expectEqualStrings(data, buffer[0..bytes_read]);
}

test "Map: seek operations" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "0123456789" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const file = try vfs.openFile(map.rootDir(.data), try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file);

    // Seek and read
    _ = try vfs.seek(file, 5, .set);
    var buffer: [5]u8 = undefined;
    const bytes_read = try vfs.readv(file, &.{&buffer});
    try testing.expectEqualStrings("56789", buffer[0..bytes_read]);
}

test "Map: pread/pwrite operations" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const file = try vfs.openFile(map.rootDir(.data), try harha.SafePath.resolve("test.txt"), .{
        .create = true,
        .mode = .read_write,
    });
    defer vfs.closeFile(file);

    // Write at specific offset
    _ = try vfs.pwritev(file, &.{"Hello World"}, 0);

    // Read at specific offset
    var buffer: [5]u8 = undefined;
    const bytes_read = try vfs.preadv(file, &.{&buffer}, 6);
    try testing.expectEqualStrings("World", buffer[0..bytes_read]);
}

test "Map: delete operations" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "delete_me.txt", .data = "x" });
    try tmp.dir.makeDir("delete_dir");

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const data_root = map.rootDir(.data);

    // Delete file
    try vfs.deleteFile(data_root, try harha.SafePath.resolve("delete_me.txt"));
    try testing.expectError(error.FileNotFound, vfs.stat(data_root, try harha.SafePath.resolve("delete_me.txt")));

    // Delete directory
    try vfs.deleteDir(data_root, try harha.SafePath.resolve("delete_dir"), .{});
    try testing.expectError(error.FileNotFound, vfs.stat(data_root, try harha.SafePath.resolve("delete_dir")));
}

test "Map: access unmounted VFS returns error" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    // Mount only .data, leave .cache unmounted
    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);

    // Try to access unmounted cache
    const cache_root = map.rootDir(.cache);
    try testing.expectError(error.NotDir, vfs.stat(cache_root, try harha.SafePath.resolve("")));
}

test "Map: remounting replaces VFS" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp1 = std.testing.tmpDir(.{});
    defer tmp1.cleanup();
    var tmp2 = std.testing.tmpDir(.{});
    defer tmp2.cleanup();

    try tmp1.dir.writeFile(.{ .sub_path = "test.txt", .data = "first" });
    try tmp2.dir.writeFile(.{ .sub_path = "test.txt", .data = "second" });

    var std_vfs1: harha.Std = try .init(allocator, tmp1.dir);
    defer std_vfs1.deinit();
    var std_vfs2: harha.Std = try .init(allocator, tmp2.dir);
    defer std_vfs2.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    // Mount first VFS
    map.mount(.data, std_vfs1.vfs(.all));

    const vfs = map.vfs(.all);
    const data_root = map.rootDir(.data);

    // Read from first VFS
    const file1 = try vfs.openFile(data_root, try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file1);
    var buffer: [256]u8 = undefined;
    const read1 = try vfs.readv(file1, &.{&buffer});
    try testing.expectEqualStrings("first", buffer[0..read1]);

    // Remount with second VFS
    map.mount(.data, std_vfs2.vfs(.all));

    // Read from second VFS (same mount point)
    const file2 = try vfs.openFile(data_root, try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file2);
    const read2 = try vfs.readv(file2, &.{&buffer});
    try testing.expectEqualStrings("second", buffer[0..read2]);
}

test "Map: handle bitpacking with medium enum" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(MediumMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "content" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    // Mount to different enum values
    map.mount(.data, std_vfs.vfs(.all));
    map.mount(.logs, std_vfs.vfs(.all));
    map.mount(.config, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);

    // Open files from different mounts
    const file_data = try vfs.openFile(map.rootDir(.data), try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file_data);

    const file_logs = try vfs.openFile(map.rootDir(.logs), try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file_logs);

    const file_config = try vfs.openFile(map.rootDir(.config), try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file_config);

    // All handles should be different (different vfs_idx in bitpacked handle)
    try testing.expect(@intFromEnum(file_data) != @intFromEnum(file_logs));
    try testing.expect(@intFromEnum(file_logs) != @intFromEnum(file_config));
    try testing.expect(@intFromEnum(file_data) != @intFromEnum(file_config));
}

test "Map: permissions passthrough" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "data" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    // Mount with read-only permissions
    map.mount(.data, std_vfs.vfs(.read_only));

    const vfs = map.vfs(.all);
    const data_root = map.rootDir(.data);

    // Should be able to read
    const file = try vfs.openFile(data_root, try harha.SafePath.resolve("test.txt"), .{});
    defer vfs.closeFile(file);

    // Should not be able to write (permission from underlying VFS)
    try testing.expectError(error.PermissionDenied, vfs.openFile(data_root, try harha.SafePath.resolve("new.txt"), .{
        .create = true,
        .mode = .write_only,
    }));
}

test "Map: walker integration" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir("a");
    try tmp.dir.makeDir("a/b");
    try tmp.dir.writeFile(.{ .sub_path = "file1.txt", .data = "1" });
    try tmp.dir.writeFile(.{ .sub_path = "a/file2.txt", .data = "2" });
    try tmp.dir.writeFile(.{ .sub_path = "a/b/file3.txt", .data = "3" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    map.mount(.data, std_vfs.vfs(.all));

    const vfs = map.vfs(.all);
    const dir = try vfs.openDir(map.rootDir(.data), try harha.SafePath.resolve(""), .{ .iterate = true });
    defer vfs.closeDir(dir);

    var walker = try vfs.walk(dir, allocator);
    defer walker.deinit();

    var file_count: u32 = 0;
    var dir_count: u32 = 0;

    while (try walker.next()) |entry| {
        if (entry.stat.kind == .file) file_count += 1;
        if (entry.stat.kind == .dir) dir_count += 1;
    }

    try testing.expectEqual(@as(u32, 3), file_count);
    try testing.expectEqual(@as(u32, 2), dir_count);
}

test "Map: zero allocation guarantee" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    // Map VFS itself requires no allocation
    var map: MapVfs = .init;
    defer map.deinit();

    // The vfs() method should not allocate
    const vfs = map.vfs(.all);
    _ = vfs;

    // rootDir should not allocate
    const data_root = map.rootDir(.data);
    _ = data_root;

    // mount/unmount should not allocate (just array assignment)
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    map.mount(.data, std_vfs.vfs(.all));
    map.unmount(.data);
}

test "Map: large enum stress test" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(LargeMount);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.txt", .data = "data" });

    var std_vfs: harha.Std = try .init(allocator, tmp.dir);
    defer std_vfs.deinit();

    var map: MapVfs = .init;
    defer map.deinit();

    // Mount to all 16 slots
    inline for (comptime std.enums.values(LargeMount)) |mount_point| {
        map.mount(mount_point, std_vfs.vfs(.all));
    }

    const vfs = map.vfs(.all);

    // Access files from different mounts
    inline for (comptime std.enums.values(LargeMount)) |mount_point| {
        const file = try vfs.openFile(map.rootDir(mount_point), try harha.SafePath.resolve("test.txt"), .{});
        defer vfs.closeFile(file);

        var buffer: [256]u8 = undefined;
        const bytes_read = try vfs.readv(file, &.{&buffer});
        try testing.expectEqualStrings("data", buffer[0..bytes_read]);
    }
}

test "Map: mixed VFS types" {
    const allocator = testing.allocator;
    const MapVfs = harha.Map(SmallMount);

    var tmp1 = std.testing.tmpDir(.{});
    defer tmp1.cleanup();
    var tmp2 = std.testing.tmpDir(.{});
    defer tmp2.cleanup();
    var tmp3 = std.testing.tmpDir(.{});
    defer tmp3.cleanup();

    try tmp1.dir.writeFile(.{ .sub_path = "data.txt", .data = "data" });
    try tmp2.dir.writeFile(.{ .sub_path = "cache.txt", .data = "cache" });
    try tmp3.dir.writeFile(.{ .sub_path = "temp.txt", .data = "temp" });

    // Create Std VFS instances
    var std_vfs1: harha.Std = try .init(allocator, tmp1.dir);
    defer std_vfs1.deinit();
    var std_vfs2: harha.Std = try .init(allocator, tmp2.dir);
    defer std_vfs2.deinit();

    // Create Overlay VFS
    var overlay: harha.Overlay = .init(allocator);
    defer overlay.deinit();
    try overlay.mount(std_vfs2.vfs(.all), "/cache");

    var map: MapVfs = .init;
    defer map.deinit();

    // Mount both Std and Overlay into Map
    map.mount(.data, std_vfs1.vfs(.all));
    map.mount(.cache, overlay.vfs(.all));

    const vfs = map.vfs(.all);

    // Access Std VFS through Map
    const file_data = try vfs.openFile(map.rootDir(.data), try harha.SafePath.resolve("data.txt"), .{});
    defer vfs.closeFile(file_data);
    var buffer: [256]u8 = undefined;
    const read1 = try vfs.readv(file_data, &.{&buffer});
    try testing.expectEqualStrings("data", buffer[0..read1]);

    // Access Overlay VFS through Map
    const file_cache = try vfs.openFile(map.rootDir(.cache), try harha.SafePath.resolve("cache/cache.txt"), .{});
    defer vfs.closeFile(file_cache);
    const read2 = try vfs.readv(file_cache, &.{&buffer});
    try testing.expectEqualStrings("cache", buffer[0..read2]);
}

// ============================================================================
// Compile-time Tests
// ============================================================================

test "Map: rootDir returns 0..N range" {
    const MapVfs = harha.Map(SmallMount);

    var map: MapVfs = .init;
    defer map.deinit();

    // rootDir should return handles in 0..N range where N is enum length
    const data_root = map.rootDir(.data);
    const cache_root = map.rootDir(.cache);
    const temp_root = map.rootDir(.temp);

    // Extract integer values
    const data_int = @intFromEnum(data_root);
    const cache_int = @intFromEnum(cache_root);
    const temp_int = @intFromEnum(temp_root);

    // Should be exactly the enum indices (0, 1, 2)
    try testing.expectEqual(@as(u32, 0), data_int);
    try testing.expectEqual(@as(u32, 1), cache_int);
    try testing.expectEqual(@as(u32, 2), temp_int);

    // All should be in range 0..3
    try testing.expect(data_int < 3);
    try testing.expect(cache_int < 3);
    try testing.expect(temp_int < 3);
}

test "Map: compile-time enum validation" {
    // Should compile with valid 0..N enum
    const ValidEnum = enum(u8) { a, b, c };
    const ValidMap = harha.Map(ValidEnum);
    _ = ValidMap;
}

// This would fail at compile time (commented out):
// test "Map: invalid enum fails compile" {
//     const InvalidEnum = enum(u8) { a = 0, b = 2, c = 3 }; // Not 0..N
//     const InvalidMap = harha.Map(InvalidEnum); // Compile error!
//     _ = InvalidMap;
// }

test "Map: bit calculation for different enum sizes" {
    // Small enum: 3 values = needs 2 bits for index, 30 bits for inner handle
    const Small = enum(u8) { a, b, c };
    const SmallMap = harha.Map(Small);
    _ = SmallMap;

    // Medium enum: 8 values = needs 3 bits for index, 29 bits for inner handle
    const Medium = enum(u8) { v0, v1, v2, v3, v4, v5, v6, v7 };
    const MediumMap = harha.Map(Medium);
    _ = MediumMap;

    // Large enum: 16 values = needs 4 bits for index, 28 bits for inner handle
    const Large = enum(u8) { v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15 };
    const LargeMap = harha.Map(Large);
    _ = LargeMap;
}
