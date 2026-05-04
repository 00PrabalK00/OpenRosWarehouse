# Developers Manual

## 1. Purpose Of This Manual

This is the developer entry point for `OpenRosWarehouse`. The full manual is intentionally split into linked pages so new contributors can move through the codebase by topic instead of reading one very long document.

The detailed pages were written from the current repository contents in `/home/next/testBuild`, including ROS 2 packages, launch files, config, web backend code, shelf/docking logic, tests, and workspace scripts.

## 2. How To Read This Manual

Start here, then jump into the section that matches the area you need to modify:

1. [Repository Overview](wiki/Developer-Manual-Repository-Overview.md)
2. [Runtime And Launch](wiki/Developer-Manual-Runtime-And-Launch.md)
3. [ROS 2 Nodes And Interfaces](wiki/Developer-Manual-ROS2-Nodes-And-Interfaces.md)
4. [Subsystems And Algorithms](wiki/Developer-Manual-Subsystems-And-Algorithms.md)
5. [Build, Test, And Workflow](wiki/Developer-Manual-Build-Test-And-Workflow.md)
6. [Debugging And Safe Modification](wiki/Developer-Manual-Debugging-And-Safe-Modification.md)
7. [Integration, Risks, And Roadmap](wiki/Developer-Manual-Integration-Risks-And-Roadmap.md)

## 3. Suggested Reading Paths

For a new developer:

1. Read [Repository Overview](wiki/Developer-Manual-Repository-Overview.md).
2. Read [Runtime And Launch](wiki/Developer-Manual-Runtime-And-Launch.md).
3. Read [ROS 2 Nodes And Interfaces](wiki/Developer-Manual-ROS2-Nodes-And-Interfaces.md).

For navigation or robot-behavior changes:

1. Read [Runtime And Launch](wiki/Developer-Manual-Runtime-And-Launch.md).
2. Read [ROS 2 Nodes And Interfaces](wiki/Developer-Manual-ROS2-Nodes-And-Interfaces.md).
3. Read [Subsystems And Algorithms](wiki/Developer-Manual-Subsystems-And-Algorithms.md).
4. Read [Debugging And Safe Modification](wiki/Developer-Manual-Debugging-And-Safe-Modification.md).

For UI or backend changes:

1. Read [Repository Overview](wiki/Developer-Manual-Repository-Overview.md).
2. Read [ROS 2 Nodes And Interfaces](wiki/Developer-Manual-ROS2-Nodes-And-Interfaces.md).
3. Read [Subsystems And Algorithms](wiki/Developer-Manual-Subsystems-And-Algorithms.md).
4. Read [Build, Test, And Workflow](wiki/Developer-Manual-Build-Test-And-Workflow.md).

For cleanup and planning work:

1. Read [Integration, Risks, And Roadmap](wiki/Developer-Manual-Integration-Risks-And-Roadmap.md).
2. Cross-check affected packages in [Repository Overview](wiki/Developer-Manual-Repository-Overview.md).

## 4. Covered Areas

The split manual covers:

- Repository layout and package ownership
- Startup flow and runtime process ownership
- Launch files, parameter loading, and stack switching
- ROS 2 nodes, topics, services, actions, and frame assumptions
- Missions, zones, safety, mapping, relocalization, docking, and shelf logic
- Flask UI and embedded ROS bridge behavior
- Hardware and sensor integration points
- Build, launch, testing, rosbag, and debugging workflows
- Safe extension patterns for nodes, services, parameters, launch arguments, and UI features
- Technical debt, risky assumptions, and a practical roadmap

## 5. Notes

- The long-form source analysis was reorganized into wiki-sized sections for readability.
- The wiki copy starts at [Developers Guide](wiki/Developers-Guide.md).
- If the codebase changes materially, update both this index page and the linked wiki pages together.
