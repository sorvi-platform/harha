const std = @import("std");

const Vfs = enum {
    native,
    sra,

    fn allEnabledList() []const u8 {
        return comptime D: {
            var comma_separated: []const u8 = &.{};
            for (std.meta.fieldNames(@This())) |name| {
                if (comma_separated.len > 0) comma_separated = comma_separated ++ ",";
                comma_separated = comma_separated ++ name;
            }
            break :D comma_separated;
        };
    }

    fn setFromList(comma_separated: []const u8) !std.enums.EnumSet(@This()) {
        var set: std.enums.EnumSet(@This()) = .initEmpty();
        var iter = std.mem.tokenizeScalar(u8, comma_separated, ',');
        while (iter.next()) |tok| {
            const vfs = std.meta.stringToEnum(@This(), tok) orelse {
                std.log.err("unknown vfs name: {s}", .{tok});
                return error.InvalidVfsName;
            };
            set.setPresent(vfs, true);
        }
        return set;
    }

    fn setupModule(self: @This(), b: *std.Build, module: *std.Build.Module) void {
        switch (self) {
            .native => {},
            .sra => {
                const dep = b.lazyDependency("sra_archive", .{
                    .target = module.resolved_target,
                    .optimize = module.optimize,
                }) orelse @panic("failed to retieve sra_archive dependency");
                module.addImport("sra", dep.module("sra"));
            },
        }
    }
};

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const vfs_list = b.option([]const u8, "vfs", "Comma separated list of vfs backends to include") orelse Vfs.allEnabledList();
    const enabled_vfs = try Vfs.setFromList(vfs_list);

    const opts = b.addOptions();
    for (std.enums.values(Vfs)) |vfs| {
        opts.addOption(bool, @tagName(vfs), enabled_vfs.contains(vfs));
    }

    const harha = b.addModule("harha", .{
        .root_source_file = b.path("src/harha.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = false,
        .imports = &.{
            .{ .name = "build_options", .module = opts.createModule() },
        },
    });

    {
        var iter = enabled_vfs.iterator();
        while (iter.next()) |vfs| vfs.setupModule(b, harha);
    }

    // TODO: static lib for C
    // TODO: provide fuse bridge

    const tst = b.addTest(.{
        .name = "harha-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    for (harha.import_table.keys(), harha.import_table.values()) |name, mod| {
        tst.root_module.addImport(name, mod);
    }

    const tst_run = b.addRunArtifact(tst);
    const tst_step = b.step("test", "run tests");
    tst_step.dependOn(&tst_run.step);
}
