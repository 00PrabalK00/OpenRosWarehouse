<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **testBuild** (8186 symbols, 28960 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/testBuild/context` | Codebase overview, check index freshness |
| `gitnexus://repo/testBuild/clusters` | All functional areas |
| `gitnexus://repo/testBuild/processes` | All execution flows |
| `gitnexus://repo/testBuild/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->

GOAL

Implement all functions and workflows shown in Scrape Data.pdf completely into the existing software.

CORE INSTRUCTIONS

1. Study Scrape Data.pdf first

Review the full PDF before making changes.

Check the text, screenshots, UI layout and feature flow shown in the PDF.

Use the images in the PDF to understand exactly where each function belongs in the software.

Do not guess the placement of features if the PDF already shows it.

2. Preserve the existing UI design

Do not create a new visual style.

Match the current software design, spacing, buttons, colors, layout patterns, font sizes and page structure.

Any new feature must feel like it was already part of the existing application.

Avoid adding UI elements that look disconnected from the rest of the system.

3. Search for existing code first

Before creating new files, functions or components, inspect the existing codebase.

Look for already implemented base logic, helper functions, API routes, UI components, state management and data models.

Reuse or extend existing functions whenever possible.

Do not duplicate logic that already exists.

4. Fully implement each feature

Do not create placeholder buttons, fake UI, dummy data or incomplete backend hooks.

Every implemented feature must be usable immediately.

Connect frontend, backend, validation, file handling, data storage and error handling wherever required.

A feature is not complete unless the user can actually use it end to end.

5. Follow the PDF feature by feature

Implement the Scrape Data.pdf functions one by one.

For each feature, identify:

Feature name
Where it appears in the PDF
Where it should appear in the existing UI
Existing code that can be reused
New code required
Frontend changes
Backend changes
Data model changes
Validation rules
Error handling
Testing steps

6. Track completion

After implementing each feature, mark it as complete in a progress checklist.

Use this format:

Feature:
Status:
Files changed:
Existing functions reused:
New functions added:
How to test:
Notes:

Only mark a feature as complete after it is fully working.

7. Avoid unnecessary redesign

Do not rewrite unrelated parts of the software.

Do not refactor large sections unless required for the Scrape Data.pdf implementation.

Do not change working behavior outside the requested feature scope.

Keep changes focused, clean and compatible with the existing structure.

8. Make the implementation production usable

Add real validation.

Handle empty states.

Handle loading states.

Handle errors clearly.

Make sure the UI does not break on small screens or different data sizes.

Keep naming consistent with the existing codebase.

Ensure the feature works after restarting the software.

9. Final output required

When finished, provide:

A completed feature checklist
A summary of all files changed
A summary of existing functions reused
A summary of new functions added
Testing instructions
Any remaining limitations, only if something truly could not be completed

IMPORTANT RULES

Do not use placeholders.

Do not skip PDF screenshots.

Do not invent a new UI style.

Do not duplicate existing functions without checking first.

Do not mark anything complete unless it works end to end.

Do not leave TODO comments instead of implementation.

The final result should be immediately usable inside the software.


Read the developers_manual.md and the wiki regarding how you have to implemnt things so that the changes actually show up. 


Use the gitnexus connector so that you can see how things are connected.

When you update things, you really dont check if a particular thing is already exisitng instead you go and make new things thats not good, I TOLD read the developers manual because thats where ur gonna find everything. How to develop further without that context ur just messing up the code base completly.
