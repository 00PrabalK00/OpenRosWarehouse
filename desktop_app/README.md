# NEXT Robot UI Desktop

This is a thin desktop shell for the existing NEXT robot web UI.

It does not rewrite the current UI. Instead, it opens the robot-hosted UI from a native desktop app and lets you choose which robot to connect to.

## What it does

- Keeps the existing `/dev`, `/user`, and `/editor` pages as they are
- Stores the target robot host, port, protocol, and page
- Supports `localhost` when running on the robot PC
- Supports a robot IP when running from a laptop on the same network

## Quick start

```bash
cd /home/next/testBuild/desktop_app
npm install
npm start
```

## Typical targets

- Robot PC local test: `http://localhost:5000/dev`
- Laptop to robot over LAN: `http://192.168.x.x:5000/dev`
- Operator-only page: `http://192.168.x.x:5000/user`

## Important note

The robot still needs to be running the existing web backend, usually `zone_web_ui`, on the target machine.

This first desktop version is intentionally low-risk: it preserves the current UI and robot backend exactly, and turns desktop mode into a clean client launcher. Later, this can be evolved into a more fully decoupled remote ROS client if you want to move more logic out of the robot-hosted backend.
