(function () {
    const CATEGORY_META = {
        shelves: { label: 'Shelves', icon: 'fas fa-boxes-stacked', singular: 'Shelf' },
        pallets: { label: 'Pallets', icon: 'fas fa-pallet', singular: 'Pallet' },
        docks: { label: 'Docks', icon: 'fas fa-warehouse', singular: 'Dock' },
        fixtures: { label: 'Fixtures', icon: 'fas fa-ruler-combined', singular: 'Fixture' },
    };

    const DEFAULT_VIEW = {
        zoom: 1,
        panX: 0,
        panY: 0,
        showGrid: true,
        showLabels: true,
        showConstraints: true,
    };

    const state = window.recognitionState = window.recognitionState || {
        initialized: false,
        domBound: false,
        loading: false,
        usingDemoData: false,
        templates: [],
        zones: {},
        activeCategory: 'shelves',
        search: '',
        toolbarStatus: 'Recognition library ready',
        activeTemplateId: '',
        editorTemplate: null,
        selectedGeometry: null,
        inspectorTab: 'properties',
        stageView: 'top',
        tool: 'select',
        viewport: { ...DEFAULT_VIEW },
        interaction: null,
        history: [],
        future: [],
        dirty: false,
        clipboardGeometry: null,
        lastValidationMessage: '',
    };

    function safeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function clone(value) {
        try {
            return JSON.parse(JSON.stringify(value));
        } catch (_err) {
            return value;
        }
    }

    function slugify(raw, fallback = 'asset') {
        const text = String(raw || '')
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, '_')
            .replace(/^_+|_+$/g, '');
        return text || fallback;
    }

    function normalizeCategory(raw) {
        const value = String(raw || '').trim().toLowerCase();
        if (value === 'shelf') return 'shelves';
        if (value === 'pallet') return 'pallets';
        if (value === 'dock') return 'docks';
        if (value === 'fixture') return 'fixtures';
        return CATEGORY_META[value] ? value : 'shelves';
    }

    function normalizePointType(raw) {
        const value = String(raw || '').trim().toLowerCase();
        if (['shelf', 'pallet', 'dock', 'fixture'].includes(value)) return value;
        return 'generic';
    }

    function isLocalTemplate(template) {
        const payload = template && typeof template === 'object' ? template : {};
        const templateId = String(payload.template_id || '').trim().toLowerCase();
        const familyKey = String(payload.family_key || '').trim().toLowerCase();
        return templateId.startsWith('local_')
            || templateId.startsWith('local_import_')
            || familyKey.startsWith('local_')
            || familyKey.startsWith('local_import_');
    }

    function formatSavedLabel(rawValue) {
        const raw = String(rawValue || '').trim();
        if (!raw) return 'Not saved yet';
        const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
        const parsed = new Date(normalized);
        if (!Number.isNaN(parsed.getTime())) {
            return parsed.toLocaleString([], {
                year: 'numeric',
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
            });
        }
        return raw.replace('T', ' ');
    }

    function buildDefaultTemplate(category = 'shelves', overrides = {}) {
        const normalizedCategory = normalizeCategory(category);
        const baseName = {
            shelves: 'Shelf Std 1200',
            pallets: 'Pallet Euro Face',
            docks: 'Dock Gate Standard',
            fixtures: 'Fixture Beacon',
        }[normalizedCategory] || 'Recognition Asset';
        const familyKey = `${slugify(normalizedCategory)}_${slugify(baseName)}`;
        const templateId = `${familyKey}__v1`;
        const defaults = {
            template_id: templateId,
            family_key: familyKey,
            name: baseName,
            category: normalizedCategory,
            geometry_type: {
                shelves: 'shelf_profile',
                pallets: 'pallet_profile',
                docks: 'dock_profile',
                fixtures: 'fixture_profile',
            }[normalizedCategory] || 'recognition_profile',
            version: 1,
            status: normalizedCategory === 'shelves' ? 'published' : 'draft',
            parent_template_id: '',
            dimensions: {
                width: normalizedCategory === 'shelves' ? 1.20 : (normalizedCategory === 'pallets' ? 0.80 : 1.10),
                depth: normalizedCategory === 'shelves' ? 0.92 : (normalizedCategory === 'pallets' ? 1.20 : 0.70),
                opening_width: normalizedCategory === 'shelves' ? 0.96 : 0.74,
                capture_depth: normalizedCategory === 'shelves' ? 0.72 : 0.64,
                clearance: 0.08,
            },
            geometry: {
                points: [
                    { id: 'entry_left', name: 'Entry Left', x: -0.48, y: -0.36, required: true, kind: 'endpoint' },
                    { id: 'entry_right', name: 'Entry Right', x: 0.48, y: -0.36, required: true, kind: 'endpoint' },
                    { id: 'rear_left', name: 'Rear Left', x: -0.48, y: 0.36, required: false, kind: 'anchor' },
                    { id: 'rear_right', name: 'Rear Right', x: 0.48, y: 0.36, required: false, kind: 'anchor' },
                    { id: 'center', name: 'Center', x: 0.0, y: 0.0, required: true, kind: 'center' },
                ],
                segments: [
                    { id: 'front_span', start: 'entry_left', end: 'entry_right', role: 'opening', distance: 0.96 },
                ],
                rectangles: [
                    { id: 'capture_zone', role: 'capture_zone', x: -0.60, y: -0.46, width: 1.20, height: 0.92 },
                ],
                annotations: [
                    { id: 'centerline', type: 'centerline', x1: 0.0, y1: -0.46, x2: 0.0, y2: 0.46 },
                ],
            },
            constraints: [
                { id: 'opening_width', type: 'distance', target: 'front_span', value: 0.96, label: 'Opening width', required: true },
                { id: 'center_lock', type: 'center_lock', target: 'center', label: 'Center lock', required: true },
            ],
            algorithm: {
                required_points: ['entry_left', 'entry_right', 'center'],
                center_strategy: 'midpoint',
                insertion_axis: 'forward_y',
            },
            validation: {},
            usage: [],
            usage_count: 0,
            notes: '',
        };
        const merged = { ...defaults, ...clone(overrides) };
        if (overrides && typeof overrides === 'object') {
            if (overrides.dimensions && typeof overrides.dimensions === 'object') {
                merged.dimensions = { ...defaults.dimensions, ...clone(overrides.dimensions) };
            }
            if (overrides.algorithm && typeof overrides.algorithm === 'object') {
                merged.algorithm = { ...defaults.algorithm, ...clone(overrides.algorithm) };
            }
            if (overrides.geometry && typeof overrides.geometry === 'object') {
                merged.geometry = {
                    points: Array.isArray(overrides.geometry.points) ? clone(overrides.geometry.points) : clone(defaults.geometry.points),
                    segments: Array.isArray(overrides.geometry.segments) ? clone(overrides.geometry.segments) : clone(defaults.geometry.segments),
                    rectangles: Array.isArray(overrides.geometry.rectangles) ? clone(overrides.geometry.rectangles) : clone(defaults.geometry.rectangles),
                    annotations: Array.isArray(overrides.geometry.annotations) ? clone(overrides.geometry.annotations) : clone(defaults.geometry.annotations),
                };
            }
            if (Array.isArray(overrides.constraints)) {
                merged.constraints = clone(overrides.constraints);
            }
        }
        return merged;
    }

    function normalizeTemplate(rawTemplate, existingTemplate = null) {
        const source = rawTemplate && typeof rawTemplate === 'object' ? clone(rawTemplate) : {};
        const seedCategory = normalizeCategory(source.category || (existingTemplate && existingTemplate.category) || 'shelves');
        const base = clone(existingTemplate || buildDefaultTemplate(seedCategory));
        const template = { ...base, ...source };
        template.category = normalizeCategory(template.category);
        template.name = String(template.name || CATEGORY_META[template.category].singular || 'Recognition Asset').trim();
        template.family_key = String(template.family_key || `${slugify(template.category)}_${slugify(template.name)}`).trim();
        const version = Number.parseInt(template.version, 10);
        template.version = Number.isFinite(version) && version > 0 ? version : 1;
        template.template_id = String(template.template_id || `${template.family_key}__v${template.version}`).trim();
        template.geometry_type = String(template.geometry_type || 'recognition_profile').trim().toLowerCase();
        template.status = ['draft', 'published', 'deprecated'].includes(String(template.status || '').trim().toLowerCase())
            ? String(template.status || '').trim().toLowerCase()
            : 'draft';
        template.parent_template_id = String(template.parent_template_id || '').trim();
        template.dimensions = {
            width: Number(template.dimensions && template.dimensions.width) || 0,
            depth: Number(template.dimensions && template.dimensions.depth) || 0,
            opening_width: Number(template.dimensions && template.dimensions.opening_width) || 0,
            capture_depth: Number(template.dimensions && template.dimensions.capture_depth) || 0,
            clearance: Number(template.dimensions && template.dimensions.clearance) || 0,
        };
        template.geometry = template.geometry && typeof template.geometry === 'object' ? template.geometry : {};
        template.geometry.points = Array.isArray(template.geometry.points) ? template.geometry.points : [];
        template.geometry.segments = Array.isArray(template.geometry.segments) ? template.geometry.segments : [];
        template.geometry.rectangles = Array.isArray(template.geometry.rectangles) ? template.geometry.rectangles : [];
        template.geometry.annotations = Array.isArray(template.geometry.annotations) ? template.geometry.annotations : [];
        template.constraints = Array.isArray(template.constraints) ? template.constraints : [];
        template.usage = Array.isArray(template.usage) ? template.usage : [];
        template.usage_count = Number(template.usage_count) || template.usage.length || 0;
        template.notes = String(template.notes || '').trim();
        template.validation = validateTemplate(template);
        return template;
    }

    function validateTemplate(template) {
        const issues = [];
        const geometry = template.geometry || {};
        const points = Array.isArray(geometry.points) ? geometry.points : [];
        const segments = Array.isArray(geometry.segments) ? geometry.segments : [];
        const rectangles = Array.isArray(geometry.rectangles) ? geometry.rectangles : [];
        const annotations = Array.isArray(geometry.annotations) ? geometry.annotations : [];
        const constraints = Array.isArray(template.constraints) ? template.constraints : [];

        function pushIssue(severity, code, title, detail, affectedGeometry, recommendedFix) {
            issues.push({
                severity,
                code,
                title,
                detail,
                affected_geometry: affectedGeometry,
                recommended_fix: recommendedFix,
            });
        }

        if (!String(template.name || '').trim()) {
            pushIssue('error', 'missing_name', 'Template name missing', 'Recognition assets need a clear name before they can be saved or published.', 'template', 'Add a template name in Properties.');
        }
        if (!String(template.geometry_type || '').trim()) {
            pushIssue('error', 'missing_geometry_type', 'Geometry type missing', 'Select the recognition geometry type expected by the runtime stack.', 'template', 'Choose a geometry type in Properties.');
        }
        if (!(Number(template.dimensions.width) > 0) || !(Number(template.dimensions.depth) > 0)) {
            pushIssue('error', 'invalid_dimensions', 'Critical dimensions incomplete', 'Width and depth must both be greater than zero.', 'dimensions', 'Set the width and depth values in the inspector.');
        }
        if (!rectangles.length && points.length < 2) {
            pushIssue('error', 'empty_geometry', 'Recognition geometry incomplete', 'The template needs at least a capture rectangle or a pair of anchor points.', 'geometry', 'Draw a rectangle or place the required points.');
        }

        const pointIds = new Set();
        const duplicates = new Set();
        let requiredPoints = 0;
        points.forEach((point) => {
            const pointId = String(point.id || '').trim();
            if (!pointId) return;
            if (pointIds.has(pointId)) duplicates.add(pointId);
            pointIds.add(pointId);
            if (point.required) requiredPoints += 1;
        });
        if (duplicates.size > 0) {
            pushIssue('error', 'duplicate_points', 'Duplicate point ids', `Duplicate ids: ${Array.from(duplicates).join(', ')}.`, 'points', 'Rename or remove the duplicated points.');
        }

        if (template.category === 'shelves') {
            if (requiredPoints < 2) {
                pushIssue('error', 'missing_required_points', 'Shelf anchors missing', 'Shelf recognition needs at least two required anchor or endpoint points.', 'points', 'Mark the critical shelf face points as required.');
            }
            const hasCenterline = annotations.some((annotation) => String(annotation && annotation.type || '').trim().toLowerCase() === 'centerline');
            if (!hasCenterline) {
                pushIssue('warning', 'ambiguous_center', 'Center calculation may be ambiguous', 'No centerline is defined for the shelf profile.', 'geometry', 'Add a centerline or center point for insertion alignment.');
            }
            if (!constraints.some((constraint) => String(constraint && constraint.type || '').trim().toLowerCase() === 'distance') && !segments.length) {
                pushIssue('warning', 'missing_span', 'Opening span not constrained', 'Shelf opening width is not captured by a segment or distance constraint.', 'constraints', 'Add a span segment or a distance constraint.');
            }
        }

        rectangles.forEach((rect) => {
            const width = Math.abs(Number(rect && rect.width) || 0);
            const height = Math.abs(Number(rect && rect.height) || 0);
            if (!(width > 0) || !(height > 0)) {
                pushIssue('error', 'broken_rectangle', 'Broken rectangle geometry', 'One of the recognition rectangles has zero or invalid size.', String(rect && rect.id || 'rectangle'), 'Resize or remove the invalid rectangle.');
            }
        });

        constraints.forEach((constraint) => {
            if (String(constraint && constraint.type || '').trim().toLowerCase() === 'distance' && !(Number(constraint && constraint.value) > 0)) {
                pushIssue('error', 'invalid_constraint', 'Constraint value invalid', 'Distance constraints must be greater than zero.', String(constraint && constraint.id || 'constraint'), 'Set a positive value or remove the constraint.');
            }
        });

        const errors = issues.filter((issue) => issue.severity === 'error').length;
        const warnings = issues.filter((issue) => issue.severity === 'warning').length;
        return {
            state: errors > 0 ? 'error' : (warnings > 0 ? 'warning' : 'ok'),
            summary: {
                errors,
                warnings,
                passes: errors > 0 ? 0 : 3,
            },
            issues,
        };
    }

    function buildDemoTemplates() {
        const shelf = buildDefaultTemplate('shelves', {
            status: 'published',
            usage_count: 3,
            usage: [
                { zone_name: 'PICK_SHELF_A1', action_id: 'lift_up', point_type: 'shelf', recognize: true },
                { zone_name: 'PICK_SHELF_B2', action_id: 'lift_down', point_type: 'shelf', recognize: true },
                { zone_name: 'DROP_SHELF_QA', action_id: 'lift_down', point_type: 'shelf', recognize: false },
            ],
            notes: 'Reference shelf used across standard pick aisles.',
        });
        const shelfCompact = buildDefaultTemplate('shelves', {
            template_id: 'shelves_shelf_compact_900__v2',
            family_key: 'shelves_shelf_compact_900',
            name: 'Shelf Compact 900',
            version: 2,
            status: 'draft',
            dimensions: {
                width: 0.90,
                depth: 0.74,
                opening_width: 0.72,
                capture_depth: 0.60,
                clearance: 0.05,
            },
            geometry: {
                points: [
                    { id: 'entry_left', name: 'Entry Left', x: -0.36, y: -0.26, required: true, kind: 'endpoint' },
                    { id: 'entry_right', name: 'Entry Right', x: 0.36, y: -0.26, required: true, kind: 'endpoint' },
                    { id: 'center', name: 'Center', x: 0.0, y: 0.0, required: true, kind: 'center' },
                ],
                segments: [
                    { id: 'front_span', start: 'entry_left', end: 'entry_right', role: 'opening', distance: 0.72 },
                ],
                rectangles: [
                    { id: 'capture_zone', role: 'capture_zone', x: -0.45, y: -0.37, width: 0.90, height: 0.74 },
                ],
                annotations: [],
            },
            constraints: [
                { id: 'opening_width', type: 'distance', target: 'front_span', value: 0.72, label: 'Opening width', required: true },
            ],
            usage_count: 0,
            usage: [],
        });
        const pallet = buildDefaultTemplate('pallets', {
            template_id: 'pallets_pallet_euro_face__v1',
            family_key: 'pallets_pallet_euro_face',
            name: 'Pallet Euro Face',
            status: 'published',
            geometry: {
                points: [
                    { id: 'face_left', name: 'Face Left', x: -0.40, y: -0.30, required: true, kind: 'endpoint' },
                    { id: 'face_right', name: 'Face Right', x: 0.40, y: -0.30, required: true, kind: 'endpoint' },
                    { id: 'center', name: 'Center', x: 0.0, y: 0.0, required: true, kind: 'center' },
                ],
                segments: [
                    { id: 'face_span', start: 'face_left', end: 'face_right', role: 'opening', distance: 0.80 },
                ],
                rectangles: [
                    { id: 'pallet_face', role: 'capture_zone', x: -0.40, y: -0.60, width: 0.80, height: 1.20 },
                ],
                annotations: [
                    { id: 'centerline', type: 'centerline', x1: 0.0, y1: -0.60, x2: 0.0, y2: 0.60 },
                ],
            },
            constraints: [
                { id: 'face_span', type: 'distance', target: 'face_span', value: 0.80, label: 'Fork entry span', required: true },
            ],
            usage_count: 0,
            usage: [],
            notes: 'Prepared for future pallet-aware pick and drop stations.',
        });
        const dock = buildDefaultTemplate('docks', {
            template_id: 'docks_dock_gate_standard__v1',
            family_key: 'docks_dock_gate_standard',
            name: 'Dock Gate Standard',
            status: 'draft',
            dimensions: {
                width: 1.10,
                depth: 0.70,
                opening_width: 0.88,
                capture_depth: 0.50,
                clearance: 0.04,
            },
            usage_count: 0,
            usage: [],
        });
        const fixture = buildDefaultTemplate('fixtures', {
            template_id: 'fixtures_fixture_beacon__v1',
            family_key: 'fixtures_fixture_beacon',
            name: 'Fixture Beacon',
            status: 'draft',
            usage_count: 0,
            usage: [],
        });
        return [shelf, shelfCompact, pallet, dock, fixture].map((item) => normalizeTemplate(item));
    }

    function getStageElement() {
        return document.getElementById('recognition-stage');
    }

    function getActiveTemplate() {
        return state.editorTemplate ? normalizeTemplate(state.editorTemplate, state.editorTemplate) : null;
    }

    function getStageMetrics() {
        const stage = getStageElement();
        const rect = stage ? stage.getBoundingClientRect() : { width: 1000, height: 700, left: 0, top: 0 };
        const baseScale = 180;
        const scale = baseScale * Math.max(0.35, Number(state.viewport.zoom) || 1);
        return {
            width: rect.width || 1000,
            height: rect.height || 700,
            left: rect.left || 0,
            top: rect.top || 0,
            scale,
            centerX: (rect.width || 1000) / 2 + (Number(state.viewport.panX) || 0),
            centerY: (rect.height || 700) / 2 + (Number(state.viewport.panY) || 0),
        };
    }

    function worldToScreen(x, y) {
        const metrics = getStageMetrics();
        return {
            x: metrics.centerX + (Number(x) * metrics.scale),
            y: metrics.centerY - (Number(y) * metrics.scale),
        };
    }

    function screenToWorld(clientX, clientY) {
        const metrics = getStageMetrics();
        return {
            x: (clientX - metrics.left - metrics.centerX) / metrics.scale,
            y: -(clientY - metrics.top - metrics.centerY) / metrics.scale,
        };
    }

    function getPointById(template, pointId) {
        const points = template && template.geometry && Array.isArray(template.geometry.points) ? template.geometry.points : [];
        return points.find((point) => String(point.id || '') === String(pointId || '')) || null;
    }

    function getSegmentWorldPoints(template, segment) {
        if (!segment) return null;
        const start = getPointById(template, segment.start);
        const end = getPointById(template, segment.end);
        if (!start || !end) return null;
        return { start, end };
    }

    function snapWorldPoint(rawWorld) {
        const template = getActiveTemplate();
        const snapped = {
            x: Math.round(Number(rawWorld.x || 0) / 0.05) * 0.05,
            y: Math.round(Number(rawWorld.y || 0) / 0.05) * 0.05,
        };
        const points = template && template.geometry && Array.isArray(template.geometry.points) ? template.geometry.points : [];
        const snapDistance = 0.08;
        points.forEach((point) => {
            const px = Number(point.x || 0);
            const py = Number(point.y || 0);
            if (Math.hypot(snapped.x - px, snapped.y - py) <= snapDistance) {
                snapped.x = px;
                snapped.y = py;
            }
            if (Math.abs(snapped.x - px) <= 0.04) {
                snapped.x = px;
            }
            if (Math.abs(snapped.y - py) <= 0.04) {
                snapped.y = py;
            }
        });
        const rectangles = template && template.geometry && Array.isArray(template.geometry.rectangles) ? template.geometry.rectangles : [];
        rectangles.forEach((rect) => {
            const x = Number(rect.x || 0);
            const y = Number(rect.y || 0);
            const width = Number(rect.width || 0);
            const height = Number(rect.height || 0);
            const edges = [x, x + width, x + width / 2];
            const rows = [y, y + height, y + height / 2];
            edges.forEach((edge) => {
                if (Math.abs(snapped.x - edge) <= 0.04) snapped.x = edge;
            });
            rows.forEach((row) => {
                if (Math.abs(snapped.y - row) <= 0.04) snapped.y = row;
            });
        });
        return {
            x: Number(snapped.x.toFixed(3)),
            y: Number(snapped.y.toFixed(3)),
        };
    }

    function generateEntityId(prefix) {
        return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
    }

    function getCurrentTemplateUsage() {
        const template = getActiveTemplate();
        if (!template) return [];
        const persisted = state.templates.find((entry) => String(entry.template_id || '') === String(template.template_id || ''));
        return clone((persisted && persisted.usage) || template.usage || []);
    }

    function pushHistory() {
        if (!state.editorTemplate) return;
        const snapshot = clone(state.editorTemplate);
        const last = state.history.length ? JSON.stringify(state.history[state.history.length - 1]) : '';
        const next = JSON.stringify(snapshot);
        if (last === next) return;
        state.history.push(snapshot);
        if (state.history.length > 60) {
            state.history.shift();
        }
        state.future = [];
    }

    function restoreFromHistory(snapshot) {
        if (!snapshot) return;
        state.editorTemplate = normalizeTemplate(snapshot, snapshot);
        state.selectedGeometry = null;
        state.dirty = true;
        renderRecognition();
    }

    function markDirty() {
        state.dirty = true;
        state.editorTemplate.validation = validateTemplate(state.editorTemplate);
        pushHistory();
        renderRecognition();
    }

    function setToolbarStatus(message) {
        state.toolbarStatus = String(message || '').trim() || 'Recognition library ready';
        const statusEl = document.getElementById('recognition-toolbar-status');
        if (statusEl) {
            statusEl.textContent = state.toolbarStatus;
        }
    }

    function pickTemplateForCategory(category) {
        const normalizedCategory = normalizeCategory(category);
        const items = state.templates.filter((template) => normalizeCategory(template.category) === normalizedCategory);
        if (!items.length) return null;
        return items[0];
    }

    function mergeTemplate(template) {
        const normalized = normalizeTemplate(template, template);
        const idx = state.templates.findIndex((entry) => String(entry.template_id || '') === String(normalized.template_id || ''));
        if (idx >= 0) {
            state.templates[idx] = normalized;
        } else {
            state.templates.unshift(normalized);
        }
        state.templates.sort((left, right) => {
            const leftCat = String(left.category || '');
            const rightCat = String(right.category || '');
            if (leftCat !== rightCat) return leftCat.localeCompare(rightCat);
            const familyCompare = String(left.family_key || '').localeCompare(String(right.family_key || ''));
            if (familyCompare !== 0) return familyCompare;
            return Number(right.version || 1) - Number(left.version || 1);
        });
        return normalized;
    }

    async function loadRecognitionData(options = {}) {
        if (state.loading) return;
        state.loading = true;
        const preserveSelection = options.preserveSelection !== false;
        const localDrafts = (state.templates || [])
            .filter((template) => isLocalTemplate(template))
            .map((template) => normalizeTemplate(template, template));
        try {
            const [templatesResp, zonesResp] = await Promise.all([
                fetch('/api/recognition/templates').then((response) => response.json()).catch(() => ({ templates: [] })),
                fetch('/api/zones').then((response) => response.json()).catch(() => ({ zones: {} })),
            ]);
            const fetchedTemplates = Array.isArray(templatesResp.templates) ? templatesResp.templates.map((item) => normalizeTemplate(item, item)) : [];
            state.usingDemoData = fetchedTemplates.length === 0;
            state.templates = fetchedTemplates.length > 0 ? fetchedTemplates : buildDemoTemplates();
            localDrafts.forEach((template) => {
                if (!state.templates.some((entry) => String(entry.template_id || '') === String(template.template_id || ''))) {
                    state.templates.unshift(template);
                }
            });
            state.zones = zonesResp && typeof zonesResp.zones === 'object' ? clone(zonesResp.zones) : {};
            const previousId = preserveSelection && state.editorTemplate ? String(state.editorTemplate.template_id || '') : '';
            const selected = previousId
                ? state.templates.find((item) => String(item.template_id || '') === previousId)
                : null;
            if (selected && !state.dirty) {
                state.editorTemplate = normalizeTemplate(selected, selected);
                state.activeTemplateId = selected.template_id;
            } else if (!state.editorTemplate) {
                const next = pickTemplateForCategory(state.activeCategory) || state.templates[0] || buildDefaultTemplate(state.activeCategory);
                state.editorTemplate = normalizeTemplate(next, next);
                state.activeTemplateId = state.editorTemplate.template_id;
                state.history = [clone(state.editorTemplate)];
                state.future = [];
                state.dirty = false;
            }
            state.initialized = true;
            renderRecognition();
            refreshActionPointForms();
        } finally {
            state.loading = false;
        }
    }

    function refreshActionPointForms() {
        ['zone', 'ez'].forEach((prefix) => {
            if (document.getElementById(`${prefix}-point-type`)) {
                refreshActionPointForm(prefix);
            }
        });
    }

    function ensureActiveTemplate() {
        if (state.editorTemplate) return state.editorTemplate;
        const template = pickTemplateForCategory(state.activeCategory) || buildDefaultTemplate(state.activeCategory);
        state.editorTemplate = normalizeTemplate(template, template);
        state.activeTemplateId = state.editorTemplate.template_id;
        state.history = [clone(state.editorTemplate)];
        state.future = [];
        return state.editorTemplate;
    }

    function createLocalTemplate(category = state.activeCategory) {
        const base = buildDefaultTemplate(category, {
            name: `${CATEGORY_META[normalizeCategory(category)].singular} Draft`,
            status: 'draft',
            template_id: `local_${slugify(category)}_${Date.now()}`,
            family_key: `local_${slugify(category)}_${Date.now()}`,
            usage: [],
            usage_count: 0,
        });
        const merged = mergeTemplate(base);
        setActiveTemplate(merged);
        state.dirty = true;
        renderRecognition();
    }

    function setActiveTemplate(template) {
        if (!template) return;
        const normalized = normalizeTemplate(template, template);
        state.editorTemplate = normalized;
        state.activeTemplateId = normalized.template_id;
        state.selectedGeometry = null;
        state.inspectorTab = 'properties';
        state.history = [clone(normalized)];
        state.future = [];
        state.dirty = false;
        renderRecognition();
        refreshActionPointForms();
    }

    function selectTemplate(templateId) {
        const next = state.templates.find((item) => String(item.template_id || '') === String(templateId || ''));
        if (!next) return;
        setActiveTemplate(next);
    }

    function findSelection(kind, id) {
        if (!state.editorTemplate || !kind || !id) return null;
        const geometry = state.editorTemplate.geometry || {};
        const list = Array.isArray(geometry[kind]) ? geometry[kind] : [];
        const target = list.find((entry) => String(entry.id || '') === String(id || ''));
        return target || null;
    }

    function deleteSelection() {
        if (!state.editorTemplate || !state.selectedGeometry) return;
        const { kind, id } = state.selectedGeometry;
        const geometry = state.editorTemplate.geometry || {};
        if (kind === 'points') {
            pushHistory();
            geometry.points = (geometry.points || []).filter((point) => String(point.id || '') !== String(id || ''));
            geometry.segments = (geometry.segments || []).filter((segment) => String(segment.start || '') !== String(id || '') && String(segment.end || '') !== String(id || ''));
            state.editorTemplate.constraints = (state.editorTemplate.constraints || []).filter((constraint) => String(constraint.target || '') !== String(id || ''));
        } else if (kind === 'segments' || kind === 'rectangles' || kind === 'annotations') {
            pushHistory();
            geometry[kind] = (geometry[kind] || []).filter((entry) => String(entry.id || '') !== String(id || ''));
            state.editorTemplate.constraints = (state.editorTemplate.constraints || []).filter((constraint) => String(constraint.target || '') !== String(id || ''));
        }
        state.selectedGeometry = null;
        markDirty();
    }

    function updateGridBackground() {
        const stage = getStageElement();
        if (!stage) return;
        const major = 120 * Math.max(0.35, Number(state.viewport.zoom) || 1);
        const minor = major / 5;
        stage.style.setProperty('--rcg-grid-major', `${major}px`);
        stage.style.setProperty('--rcg-grid-minor', `${minor}px`);
        stage.style.setProperty('--rcg-grid-x', `${Number(state.viewport.panX) || 0}px`);
        stage.style.setProperty('--rcg-grid-y', `${Number(state.viewport.panY) || 0}px`);
        stage.classList.toggle('grid-hidden', !state.viewport.showGrid);
        stage.dataset.tool = state.tool;
    }

    function renderDimensionBadge(x, y, label, tone = 'default') {
        return `
            <g class="rcg-dim-badge">
                <rect x="${x - 28}" y="${y - 12}" width="56" height="20" rx="10" fill="${tone === 'warning' ? 'rgba(120, 78, 18, 0.72)' : 'rgba(10, 18, 29, 0.82)'}" stroke="${tone === 'warning' ? 'rgba(250, 204, 21, 0.36)' : 'rgba(112, 137, 172, 0.22)'}"></rect>
                <text x="${x}" y="${y + 3}" fill="${tone === 'warning' ? '#facc15' : '#dce7f8'}" font-size="10" font-family="Roboto Mono, monospace" font-weight="700" text-anchor="middle">${safeHtml(label)}</text>
            </g>
        `;
    }

    function renderConstraintBadge(x, y, label) {
        return `
            <g class="rcg-constraint-badge">
                <rect x="${x - 34}" y="${y - 11}" width="68" height="18" rx="9" fill="rgba(25, 51, 84, 0.82)" stroke="rgba(126, 186, 255, 0.3)"></rect>
                <text x="${x}" y="${y + 2}" fill="#ebf6ff" font-size="9" font-weight="700" text-anchor="middle">${safeHtml(label)}</text>
            </g>
        `;
    }

    function resolveIssuePoint(issue) {
        const template = getActiveTemplate();
        if (!template) return { x: 42, y: 42 };
        const affected = String(issue && issue.affected_geometry || '').trim();
        if (!affected) return { x: 42, y: 42 };
        const point = getPointById(template, affected);
        if (point) return worldToScreen(point.x, point.y);
        const rect = (template.geometry.rectangles || []).find((entry) => String(entry.id || '') === affected);
        if (rect) return worldToScreen(Number(rect.x || 0) + (Number(rect.width || 0) / 2), Number(rect.y || 0));
        return { x: 42, y: 42 };
    }

    function renderStage() {
        const template = getActiveTemplate();
        const svg = document.getElementById('recognition-svg');
        const emptyState = document.getElementById('recognition-empty-state');
        if (!svg || !template) return;
        updateGridBackground();

        const points = Array.isArray(template.geometry.points) ? template.geometry.points : [];
        const segments = Array.isArray(template.geometry.segments) ? template.geometry.segments : [];
        const rectangles = Array.isArray(template.geometry.rectangles) ? template.geometry.rectangles : [];
        const annotations = Array.isArray(template.geometry.annotations) ? template.geometry.annotations : [];
        let html = '';

        rectangles.forEach((rect) => {
            const topLeft = worldToScreen(Number(rect.x || 0), Number(rect.y || 0));
            const bottomRight = worldToScreen(Number(rect.x || 0) + Number(rect.width || 0), Number(rect.y || 0) + Number(rect.height || 0));
            const widthPx = Math.abs(bottomRight.x - topLeft.x);
            const heightPx = Math.abs(bottomRight.y - topLeft.y);
            const x = Math.min(topLeft.x, bottomRight.x);
            const y = Math.min(topLeft.y, bottomRight.y);
            const selected = state.selectedGeometry && state.selectedGeometry.kind === 'rectangles' && state.selectedGeometry.id === rect.id;
            html += `
                <rect
                    class="rcg-geometry-shape${selected ? ' selected' : ''}"
                    data-geo-kind="rectangles"
                    data-geo-id="${safeHtml(rect.id)}"
                    x="${x}"
                    y="${y}"
                    width="${widthPx}"
                    height="${heightPx}"
                    rx="18"
                    fill="rgba(39, 84, 146, 0.16)"
                    stroke="${selected ? '#8ec5ff' : 'rgba(105, 171, 255, 0.44)'}"
                    stroke-width="${selected ? 2.4 : 1.4}"
                    stroke-dasharray="${rect.role === 'capture_zone' ? '12 8' : '0'}"
                ></rect>
            `;
            if (state.viewport.showLabels) {
                const widthLabel = `${Number(rect.width || 0).toFixed(2)}m`;
                const heightLabel = `${Number(rect.height || 0).toFixed(2)}m`;
                html += renderDimensionBadge(x + (widthPx / 2), y - 18, widthLabel);
                html += renderDimensionBadge(x + widthPx + 32, y + (heightPx / 2), heightLabel);
            }
        });

        annotations.forEach((annotation) => {
            if (String(annotation.type || '').trim().toLowerCase() !== 'centerline') return;
            const start = worldToScreen(annotation.x1 || 0, annotation.y1 || 0);
            const end = worldToScreen(annotation.x2 || 0, annotation.y2 || 0);
            const selected = state.selectedGeometry && state.selectedGeometry.kind === 'annotations' && state.selectedGeometry.id === annotation.id;
            html += `
                <line
                    class="rcg-geometry-shape${selected ? ' selected' : ''}"
                    data-geo-kind="annotations"
                    data-geo-id="${safeHtml(annotation.id)}"
                    x1="${start.x}"
                    y1="${start.y}"
                    x2="${end.x}"
                    y2="${end.y}"
                    stroke="${selected ? '#f8fafc' : 'rgba(126, 186, 255, 0.44)'}"
                    stroke-width="${selected ? 2.2 : 1.2}"
                    stroke-dasharray="8 7"
                ></line>
            `;
        });

        segments.forEach((segment) => {
            const world = getSegmentWorldPoints(template, segment);
            if (!world) return;
            const start = worldToScreen(world.start.x, world.start.y);
            const end = worldToScreen(world.end.x, world.end.y);
            const selected = state.selectedGeometry && state.selectedGeometry.kind === 'segments' && state.selectedGeometry.id === segment.id;
            const tone = segment.role === 'opening' ? '#ffb347' : '#6fb9ff';
            html += `
                <line
                    class="rcg-geometry-shape${selected ? ' selected' : ''}"
                    data-geo-kind="segments"
                    data-geo-id="${safeHtml(segment.id)}"
                    x1="${start.x}"
                    y1="${start.y}"
                    x2="${end.x}"
                    y2="${end.y}"
                    stroke="${selected ? '#ffffff' : tone}"
                    stroke-width="${selected ? 3.2 : 2.2}"
                    stroke-linecap="round"
                ></line>
            `;
            if (state.viewport.showLabels) {
                const distance = Number(segment.distance || Math.hypot(Number(world.end.x || 0) - Number(world.start.x || 0), Number(world.end.y || 0) - Number(world.start.y || 0)));
                html += renderDimensionBadge((start.x + end.x) / 2, (start.y + end.y) / 2 - 16, `${distance.toFixed(2)}m`);
            }
        });

        points.forEach((point) => {
            const screen = worldToScreen(point.x, point.y);
            const selected = state.selectedGeometry && state.selectedGeometry.kind === 'points' && state.selectedGeometry.id === point.id;
            const pointColor = point.kind === 'center'
                ? '#c084fc'
                : (point.kind === 'endpoint' ? '#ffb347' : '#60a5fa');
            html += `
                <g
                    class="rcg-geometry-shape${selected ? ' selected' : ''}"
                    data-geo-kind="points"
                    data-geo-id="${safeHtml(point.id)}"
                >
                    <circle cx="${screen.x}" cy="${screen.y}" r="${selected ? 8 : 6}" fill="rgba(10, 16, 24, 0.95)" stroke="${pointColor}" stroke-width="${selected ? 3 : 2}"></circle>
                    <circle cx="${screen.x}" cy="${screen.y}" r="2" fill="${pointColor}"></circle>
                </g>
            `;
            if (state.viewport.showLabels) {
                html += `<text x="${screen.x + 10}" y="${screen.y - 8}" fill="#dce7f8" font-size="11" font-weight="700">${safeHtml(point.name || point.id)}</text>`;
            }
        });

        if (state.viewport.showConstraints) {
            (template.constraints || []).forEach((constraint) => {
                const targetId = String(constraint.target || '').trim();
                let anchor = null;
                const point = getPointById(template, targetId);
                if (point) anchor = worldToScreen(point.x, point.y);
                if (!anchor) {
                    const rect = (template.geometry.rectangles || []).find((item) => String(item.id || '') === targetId);
                    if (rect) anchor = worldToScreen(Number(rect.x || 0) + Number(rect.width || 0), Number(rect.y || 0));
                }
                if (!anchor) {
                    const segment = (template.geometry.segments || []).find((item) => String(item.id || '') === targetId);
                    const world = segment ? getSegmentWorldPoints(template, segment) : null;
                    if (world) anchor = worldToScreen((Number(world.start.x || 0) + Number(world.end.x || 0)) / 2, (Number(world.start.y || 0) + Number(world.end.y || 0)) / 2);
                }
                if (!anchor) return;
                html += renderConstraintBadge(anchor.x, anchor.y + 24, constraint.label || constraint.type || 'Constraint');
            });
        }

        const issues = template.validation && Array.isArray(template.validation.issues) ? template.validation.issues.slice(0, 6) : [];
        issues.forEach((issue) => {
            const marker = resolveIssuePoint(issue);
            const color = issue.severity === 'error' ? '#f87171' : '#facc15';
            html += `
                <g class="rcg-validation-badge">
                    <circle cx="${marker.x}" cy="${marker.y}" r="11" fill="rgba(11, 17, 26, 0.9)" stroke="${color}" stroke-width="2"></circle>
                    <text x="${marker.x}" y="${marker.y + 4}" fill="${color}" font-size="10" font-weight="800" text-anchor="middle">${issue.severity === 'error' ? '!' : 'i'}</text>
                </g>
            `;
        });

        if (state.interaction && state.interaction.preview) {
            const preview = state.interaction.preview;
            if (preview.type === 'rectangle') {
                const start = worldToScreen(preview.start.x, preview.start.y);
                const end = worldToScreen(preview.end.x, preview.end.y);
                html += `
                    <rect
                        x="${Math.min(start.x, end.x)}"
                        y="${Math.min(start.y, end.y)}"
                        width="${Math.abs(end.x - start.x)}"
                        height="${Math.abs(end.y - start.y)}"
                        rx="16"
                        fill="rgba(55, 123, 216, 0.1)"
                        stroke="rgba(135, 191, 255, 0.72)"
                        stroke-width="2"
                        stroke-dasharray="10 8"
                    ></rect>
                `;
            } else if (preview.type === 'segment') {
                const start = worldToScreen(preview.start.x, preview.start.y);
                const end = worldToScreen(preview.end.x, preview.end.y);
                html += `
                    <line
                        x1="${start.x}"
                        y1="${start.y}"
                        x2="${end.x}"
                        y2="${end.y}"
                        stroke="rgba(255, 179, 71, 0.84)"
                        stroke-width="2.6"
                        stroke-dasharray="10 7"
                        stroke-linecap="round"
                    ></line>
                `;
            }
        }

        svg.innerHTML = html;
        if (emptyState) {
            emptyState.classList.toggle('visible', !rectangles.length && !points.length);
        }
    }

    function renderTopToolbar() {
        const template = getActiveTemplate();
        const card = document.getElementById('recognition-toolbar-card');
        if (card) {
            if (!template) {
                card.innerHTML = '<i class="fas fa-eye"></i><div class="recognition-toolbar-card-meta"><span class="recognition-toolbar-card-title">No template</span><span class="recognition-toolbar-card-sub">Create or select a recognition asset</span></div>';
            } else {
                const validation = template.validation || { summary: {} };
                const summary = validation.summary || {};
                const validationChip = summary.errors > 0
                    ? `<span class="recognition-toolbar-chip error">${summary.errors} error${summary.errors === 1 ? '' : 's'}</span>`
                    : (summary.warnings > 0
                        ? `<span class="recognition-toolbar-chip warn">${summary.warnings} warn${summary.warnings === 1 ? '' : 's'}</span>`
                        : '<span class="recognition-toolbar-chip success">ready</span>');
                const usageCount = Number(template.usage_count) || getCurrentTemplateUsage().length || 0;
                const savedLabel = isLocalTemplate(template)
                    ? 'Local draft'
                    : `Saved ${safeHtml(formatSavedLabel(template.updated_at))}`;
                card.innerHTML = `
                    <i class="${CATEGORY_META[normalizeCategory(template.category)].icon}"></i>
                    <div class="recognition-toolbar-card-meta">
                        <span class="recognition-toolbar-card-title">${safeHtml(template.name)}</span>
                        <span class="recognition-toolbar-card-sub">
                            <span>${safeHtml(CATEGORY_META[normalizeCategory(template.category)].singular)}</span>
                            <span>v${Number(template.version || 1)}</span>
                            <span class="recognition-toolbar-chip ${template.status === 'published' ? 'success' : (template.status === 'deprecated' ? 'error' : '')}">${safeHtml(template.status || 'draft')}</span>
                            <span>${usageCount} use${usageCount === 1 ? '' : 's'}</span>
                            <span>${savedLabel}</span>
                            ${validationChip}
                        </span>
                    </div>
                `;
            }
        }
        setToolbarStatus(state.toolbarStatus);
    }

    function renderLibrary() {
        const container = document.getElementById('recognition-library-content');
        if (!container) return;
        const category = normalizeCategory(state.activeCategory);
        const search = String(state.search || '').trim().toLowerCase();
        const items = state.templates
            .filter((template) => normalizeCategory(template.category) === category)
            .filter((template) => {
                if (!search) return true;
                return `${template.name} ${template.geometry_type} ${template.status}`.toLowerCase().includes(search);
            });

        if (!items.length) {
            container.innerHTML = `
                <div class="rcg-library-group">
                    <div class="rcg-library-group-header"><span>${safeHtml(CATEGORY_META[category].label)}</span><span>0</span></div>
                    <div class="rcg-library-card">
                        <div class="rcg-library-card-name">No templates yet</div>
                        <div class="rcg-library-card-meta">
                            <span>Create a new ${safeHtml(CATEGORY_META[category].singular.toLowerCase())} template or import one from JSON.</span>
                        </div>
                    </div>
                </div>
            `;
            return;
        }

        const groups = {
            published: items.filter((item) => item.status === 'published'),
            draft: items.filter((item) => item.status === 'draft'),
            deprecated: items.filter((item) => item.status === 'deprecated'),
        };

        container.innerHTML = Object.entries(groups)
            .filter(([, groupItems]) => groupItems.length > 0)
            .map(([groupName, groupItems]) => `
                <div class="rcg-library-group">
                    <div class="rcg-library-group-header">
                        <span>${safeHtml(groupName)}</span>
                        <span>${groupItems.length}</span>
                    </div>
                    ${groupItems.map((template) => {
                        const validation = template.validation && template.validation.summary ? template.validation.summary : { errors: 0, warnings: 0 };
                        const active = state.editorTemplate && String(state.editorTemplate.template_id || '') === String(template.template_id || '');
                        return `
                            <div class="rcg-library-card${active ? ' active' : ''}" onclick="recognitionSelectTemplate('${safeHtml(template.template_id)}')">
                                <div class="rcg-library-card-head">
                                    <div>
                                        <div class="rcg-library-card-name">${safeHtml(template.name)}</div>
                                        <div class="rcg-library-card-meta">
                                            <span class="recognition-toolbar-chip ${template.status === 'published' ? 'success' : (template.status === 'deprecated' ? 'error' : '')}">${safeHtml(template.status)}</span>
                                            <span>v${Number(template.version || 1)}</span>
                                            <span>${safeHtml(template.geometry_type || 'profile')}</span>
                                            <span>${Number(template.usage_count || 0)} use${Number(template.usage_count || 0) === 1 ? '' : 's'}</span>
                                        </div>
                                    </div>
                                    <div class="rcg-library-card-meta">
                                        ${validation.errors > 0 ? `<span class="recognition-toolbar-chip error">${validation.errors} err</span>` : ''}
                                        ${validation.errors === 0 && validation.warnings > 0 ? `<span class="recognition-toolbar-chip warn">${validation.warnings} warn</span>` : ''}
                                    </div>
                                </div>
                                <div class="rcg-library-card-actions">
                                    <button type="button" class="rcg-mini-btn" onclick="event.stopPropagation(); recognitionCopyGeometry('${safeHtml(template.template_id)}')"><i class="fas fa-copy"></i><span>Copy</span></button>
                                    <button type="button" class="rcg-mini-btn" onclick="event.stopPropagation(); recognitionDuplicateActive('${safeHtml(template.template_id)}')"><i class="fas fa-clone"></i><span>Duplicate</span></button>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            `)
            .join('');
    }

    function renderStageHeader() {
        const template = getActiveTemplate();
        const titleEl = document.getElementById('recognition-stage-title');
        const subtitleEl = document.getElementById('recognition-stage-subtitle');
        const statusEl = document.getElementById('recognition-stage-status');
        if (titleEl) {
            titleEl.textContent = template ? template.name : 'Recognition Geometry';
        }
        if (subtitleEl) {
            if (!template) {
                subtitleEl.textContent = 'Create or select a recognition asset from the library to start drawing.';
            } else {
                const points = template.geometry && Array.isArray(template.geometry.points) ? template.geometry.points.length : 0;
                const rectangles = template.geometry && Array.isArray(template.geometry.rectangles) ? template.geometry.rectangles.length : 0;
                const segments = template.geometry && Array.isArray(template.geometry.segments) ? template.geometry.segments.length : 0;
                subtitleEl.textContent = `${CATEGORY_META[normalizeCategory(template.category)].singular} recognition geometry · ${rectangles} rectangle${rectangles === 1 ? '' : 's'} · ${points} point${points === 1 ? '' : 's'} · ${segments} span${segments === 1 ? '' : 's'}`;
            }
        }
        if (statusEl) {
            const validation = template && template.validation ? template.validation.summary : { errors: 0, warnings: 0 };
            const modeText = {
                select: 'Select or drag existing geometry. Scroll to zoom. Drag empty canvas to pan.',
                point: 'Place recognition points with grid and anchor snapping.',
                endpoint: 'Place required endpoints for the recognition pattern.',
                rectangle: 'Drag to define a recognition capture rectangle.',
                segment: 'Drag to define a measured span between two auto-created points.',
            }[state.tool] || 'Recognition editor ready.';
            const validationText = template
                ? ` ${validation.errors > 0 ? `${validation.errors} blocking error(s).` : (validation.warnings > 0 ? `${validation.warnings} warning(s).` : 'Validation looks clean.')}`
                : '';
            statusEl.textContent = `${modeText}${validationText}`;
        }
    }

    function renderInspector() {
        const container = document.getElementById('recognition-inspector-content');
        if (!container) return;
        const template = getActiveTemplate();
        const validation = template && template.validation ? template.validation : { summary: { errors: 0, warnings: 0 }, issues: [] };
        const summary = validation.summary || { errors: 0, warnings: 0, passes: 0 };
        const usage = template ? getCurrentTemplateUsage() : [];
        const selected = state.selectedGeometry ? findSelection(state.selectedGeometry.kind, state.selectedGeometry.id) : null;

        if (!template) {
            container.innerHTML = `
                <div class="rcg-inspector-scroll">
                    <div class="rcg-inspector-card">
                        <div class="rcg-card-kicker">Recognition</div>
                        <div class="rcg-card-title">No active template</div>
                        <div class="rcg-pane-subtitle">Create a new template or select one from the library to begin.</div>
                    </div>
                </div>
            `;
            return;
        }

        if (state.inspectorTab === 'properties') {
            const safeVersioningNote = template.status === 'published' && usage.length > 0
                ? '<div class="rcg-warning-text" style="margin-top:10px;">Live action points reference this template. Publishing changes will create a new safe version.</div>'
                : '';
            const databaseState = isLocalTemplate(template)
                ? 'Local Draft'
                : String(template.status || 'draft').replace(/^./, (value) => value.toUpperCase());
            const savedLabel = isLocalTemplate(template) ? 'Pending first save' : formatSavedLabel(template.updated_at);
            container.innerHTML = `
                <div class="rcg-inspector-scroll">
                    <div class="rcg-inspector-card">
                        <div class="rcg-card-kicker">Template</div>
                        <div class="rcg-card-title">${safeHtml(template.name)}</div>
                        <div class="rcg-pane-subtitle">${safeHtml(CATEGORY_META[normalizeCategory(template.category)].label)} recognition asset stored with version-aware deployment safety.</div>
                        <div class="rcg-stat-grid">
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Version</span><span class="rcg-stat-value">v${Number(template.version || 1)}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Usage</span><span class="rcg-stat-value">${usage.length}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Errors</span><span class="rcg-stat-value">${summary.errors}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Warnings</span><span class="rcg-stat-value">${summary.warnings}</span></div>
                        </div>
                        ${safeVersioningNote}
                    </div>
                    <div class="rcg-inspector-card">
                        <div class="rcg-card-kicker">Properties</div>
                        <div class="rcg-form-grid">
                            <div class="rcg-form-field">
                                <label>Name</label>
                                <input class="rcg-form-input" value="${safeHtml(template.name)}" oninput="recognitionUpdateTemplateField('name', this.value)">
                            </div>
                            <div class="rcg-form-field">
                                <label>Category</label>
                                <select class="rcg-form-select" onchange="recognitionUpdateTemplateField('category', this.value)">
                                    ${Object.keys(CATEGORY_META).map((key) => `<option value="${key}"${normalizeCategory(template.category) === key ? ' selected' : ''}>${CATEGORY_META[key].label}</option>`).join('')}
                                </select>
                            </div>
                            <div class="rcg-form-field">
                                <label>Geometry Type</label>
                                <input class="rcg-form-input" value="${safeHtml(template.geometry_type)}" oninput="recognitionUpdateTemplateField('geometry_type', this.value)">
                            </div>
                            <div class="rcg-form-field">
                                <label>Database State</label>
                                <div class="rcg-readonly-field">${safeHtml(databaseState)}</div>
                            </div>
                            <div class="rcg-form-field">
                                <label>Last Saved</label>
                                <div class="rcg-readonly-field">${safeHtml(savedLabel)}</div>
                            </div>
                            <div class="rcg-form-field">
                                <label>Template ID</label>
                                <div class="rcg-readonly-field rcg-readonly-mono">${safeHtml(template.template_id || 'local draft')}</div>
                            </div>
                        </div>
                    </div>
                    <div class="rcg-inspector-card">
                        <div class="rcg-card-kicker">Critical Dimensions</div>
                        <div class="rcg-form-grid">
                            <div class="rcg-form-field">
                                <label>Width (m)</label>
                                <input class="rcg-form-input" type="number" step="0.01" value="${Number(template.dimensions.width || 0).toFixed(2)}" oninput="recognitionUpdateTemplateField('dimensions.width', this.value)">
                            </div>
                            <div class="rcg-form-field">
                                <label>Depth (m)</label>
                                <input class="rcg-form-input" type="number" step="0.01" value="${Number(template.dimensions.depth || 0).toFixed(2)}" oninput="recognitionUpdateTemplateField('dimensions.depth', this.value)">
                            </div>
                            <div class="rcg-form-field">
                                <label>Opening Width (m)</label>
                                <input class="rcg-form-input" type="number" step="0.01" value="${Number(template.dimensions.opening_width || 0).toFixed(2)}" oninput="recognitionUpdateTemplateField('dimensions.opening_width', this.value)">
                            </div>
                            <div class="rcg-form-field">
                                <label>Capture Depth (m)</label>
                                <input class="rcg-form-input" type="number" step="0.01" value="${Number(template.dimensions.capture_depth || 0).toFixed(2)}" oninput="recognitionUpdateTemplateField('dimensions.capture_depth', this.value)">
                            </div>
                        </div>
                    </div>
                    <div class="rcg-inspector-card">
                        <div class="rcg-card-kicker">Notes</div>
                        <div class="rcg-form-grid single">
                            <div class="rcg-form-field">
                                <label>Deployment Notes</label>
                                <textarea class="rcg-form-textarea" oninput="recognitionUpdateTemplateField('notes', this.value)">${safeHtml(template.notes || '')}</textarea>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        } else if (state.inspectorTab === 'geometry') {
            const counts = {
                points: Array.isArray(template.geometry.points) ? template.geometry.points.length : 0,
                segments: Array.isArray(template.geometry.segments) ? template.geometry.segments.length : 0,
                rectangles: Array.isArray(template.geometry.rectangles) ? template.geometry.rectangles.length : 0,
                annotations: Array.isArray(template.geometry.annotations) ? template.geometry.annotations.length : 0,
            };
            let selectedCard = `
                <div class="rcg-inspector-card">
                    <div class="rcg-card-kicker">Selection</div>
                    <div class="rcg-card-title">Nothing selected</div>
                    <div class="rcg-pane-subtitle">Select a point, rectangle, segment, or centerline to edit it numerically.</div>
                </div>
            `;
            if (selected) {
                const kind = state.selectedGeometry.kind;
                if (kind === 'points') {
                    selectedCard = `
                        <div class="rcg-inspector-card">
                            <div class="rcg-card-kicker">Selected Point</div>
                            <div class="rcg-card-title">${safeHtml(selected.name || selected.id)}</div>
                            <div class="rcg-form-grid">
                                <div class="rcg-form-field">
                                    <label>X (m)</label>
                                    <input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.x || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('x', this.value)">
                                </div>
                                <div class="rcg-form-field">
                                    <label>Y (m)</label>
                                    <input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.y || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('y', this.value)">
                                </div>
                                <div class="rcg-form-field">
                                    <label>Name</label>
                                    <input class="rcg-form-input" value="${safeHtml(selected.name || selected.id)}" oninput="recognitionUpdateSelectedGeometryField('name', this.value)">
                                </div>
                                <div class="rcg-form-field">
                                    <label>Kind</label>
                                    <select class="rcg-form-select" onchange="recognitionUpdateSelectedGeometryField('kind', this.value)">
                                        <option value="anchor"${selected.kind === 'anchor' ? ' selected' : ''}>Anchor</option>
                                        <option value="endpoint"${selected.kind === 'endpoint' ? ' selected' : ''}>Endpoint</option>
                                        <option value="center"${selected.kind === 'center' ? ' selected' : ''}>Center</option>
                                    </select>
                                </div>
                            </div>
                            <div style="margin-top:12px;display:flex;align-items:center;justify-content:space-between;gap:10px;">
                                <div class="rcg-dimension-pill">Required for solve</div>
                                <label style="display:flex;align-items:center;gap:8px;color:var(--text-secondary);font-size:11px;">
                                    <input type="checkbox" ${selected.required ? 'checked' : ''} onchange="recognitionUpdateSelectedGeometryField('required', this.checked)">
                                    Required
                                </label>
                            </div>
                            <div style="margin-top:12px;">
                                <button type="button" class="rcg-inspector-btn" onclick="recognitionDeleteSelection()"><i class="fas fa-trash"></i><span>Delete Selection</span></button>
                            </div>
                        </div>
                    `;
                } else if (kind === 'rectangles') {
                    selectedCard = `
                        <div class="rcg-inspector-card">
                            <div class="rcg-card-kicker">Selected Rectangle</div>
                            <div class="rcg-card-title">${safeHtml(selected.role || selected.id)}</div>
                            <div class="rcg-form-grid">
                                <div class="rcg-form-field"><label>X (m)</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.x || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('x', this.value)"></div>
                                <div class="rcg-form-field"><label>Y (m)</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.y || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('y', this.value)"></div>
                                <div class="rcg-form-field"><label>Width (m)</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.width || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('width', this.value)"></div>
                                <div class="rcg-form-field"><label>Height (m)</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.height || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('height', this.value)"></div>
                                <div class="rcg-form-field"><label>Role</label><input class="rcg-form-input" value="${safeHtml(selected.role || '')}" oninput="recognitionUpdateSelectedGeometryField('role', this.value)"></div>
                            </div>
                            <div style="margin-top:12px;">
                                <button type="button" class="rcg-inspector-btn" onclick="recognitionDeleteSelection()"><i class="fas fa-trash"></i><span>Delete Selection</span></button>
                            </div>
                        </div>
                    `;
                } else if (kind === 'segments') {
                    selectedCard = `
                        <div class="rcg-inspector-card">
                            <div class="rcg-card-kicker">Selected Span</div>
                            <div class="rcg-card-title">${safeHtml(selected.role || selected.id)}</div>
                            <div class="rcg-form-grid">
                                <div class="rcg-form-field"><label>Label</label><input class="rcg-form-input" value="${safeHtml(selected.role || '')}" oninput="recognitionUpdateSelectedGeometryField('role', this.value)"></div>
                                <div class="rcg-form-field"><label>Distance (m)</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.distance || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('distance', this.value)"></div>
                            </div>
                            <div style="margin-top:12px;">
                                <button type="button" class="rcg-inspector-btn" onclick="recognitionDeleteSelection()"><i class="fas fa-trash"></i><span>Delete Selection</span></button>
                            </div>
                        </div>
                    `;
                } else if (kind === 'annotations') {
                    selectedCard = `
                        <div class="rcg-inspector-card">
                            <div class="rcg-card-kicker">Selected Annotation</div>
                            <div class="rcg-card-title">${safeHtml(selected.type || selected.id)}</div>
                            <div class="rcg-form-grid">
                                <div class="rcg-form-field"><label>X1</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.x1 || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('x1', this.value)"></div>
                                <div class="rcg-form-field"><label>Y1</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.y1 || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('y1', this.value)"></div>
                                <div class="rcg-form-field"><label>X2</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.x2 || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('x2', this.value)"></div>
                                <div class="rcg-form-field"><label>Y2</label><input class="rcg-form-input" type="number" step="0.01" value="${Number(selected.y2 || 0).toFixed(2)}" oninput="recognitionUpdateSelectedGeometryField('y2', this.value)"></div>
                            </div>
                            <div style="margin-top:12px;">
                                <button type="button" class="rcg-inspector-btn" onclick="recognitionDeleteSelection()"><i class="fas fa-trash"></i><span>Delete Selection</span></button>
                            </div>
                        </div>
                    `;
                }
            }

            container.innerHTML = `
                <div class="rcg-inspector-scroll">
                    ${selectedCard}
                    <div class="rcg-inspector-card">
                        <div class="rcg-card-kicker">Geometry Inventory</div>
                        <div class="rcg-stat-grid">
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Points</span><span class="rcg-stat-value">${counts.points}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Spans</span><span class="rcg-stat-value">${counts.segments}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Rectangles</span><span class="rcg-stat-value">${counts.rectangles}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Annotations</span><span class="rcg-stat-value">${counts.annotations}</span></div>
                        </div>
                    </div>
                    <div class="rcg-inspector-card">
                        <div class="rcg-card-kicker">Constraint Editing</div>
                        <div class="rcg-pane-subtitle">Keep constraints lightweight: enough to lock critical recognition spans without turning this into full CAD.</div>
                        <div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap;">
                            <button type="button" class="rcg-inspector-btn" onclick="recognitionAddConstraint('distance')"><i class="fas fa-ruler-horizontal"></i><span>Add Distance</span></button>
                            <button type="button" class="rcg-inspector-btn" onclick="recognitionAddConstraint('center_lock')"><i class="fas fa-crosshairs"></i><span>Add Center Lock</span></button>
                        </div>
                        <div style="margin-top:14px;">
                            ${(template.constraints || []).map((constraint) => `
                                <div class="rcg-issue-row">
                                    <div>
                                        <div class="rcg-issue-title">${safeHtml(constraint.label || constraint.type || 'Constraint')}</div>
                                        <div class="rcg-issue-text">${safeHtml(String(constraint.target || 'template'))}${constraint.value ? ` · ${safeHtml(Number(constraint.value).toFixed(2))}m` : ''}</div>
                                    </div>
                                    <button type="button" class="rcg-mini-btn" onclick="recognitionRemoveConstraint('${safeHtml(constraint.id || '')}')"><i class="fas fa-times"></i><span>Remove</span></button>
                                </div>
                            `).join('') || '<div class="rcg-pane-subtitle" style="margin-top:12px;">No constraints yet.</div>'}
                        </div>
                    </div>
                </div>
            `;
        } else if (state.inspectorTab === 'validation') {
            container.innerHTML = `
                <div class="rcg-inspector-scroll">
                    <div class="rcg-validation-card">
                        <div class="rcg-card-kicker">Validation</div>
                        <div class="rcg-card-title">${summary.errors > 0 ? 'Blocking issues detected' : (summary.warnings > 0 ? 'Validation warnings only' : 'Template is valid')}</div>
                        <div class="rcg-stat-grid">
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Errors</span><span class="rcg-stat-value">${summary.errors}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Warnings</span><span class="rcg-stat-value">${summary.warnings}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">Passes</span><span class="rcg-stat-value">${summary.passes || 0}</span></div>
                            <div class="rcg-stat-tile"><span class="rcg-stat-label">State</span><span class="rcg-stat-value">${safeHtml(validation.state || 'ok')}</span></div>
                        </div>
                    </div>
                    <div class="rcg-validation-card">
                        <div class="rcg-card-kicker">Issue List</div>
                        ${(validation.issues || []).map((issue) => `
                            <div class="rcg-issue-row">
                                <div style="display:flex;gap:10px;align-items:flex-start;">
                                    <span class="rcg-status-dot ${issue.severity === 'error' ? 'error' : (issue.severity === 'warning' ? 'warn' : 'ok')}"></span>
                                    <div>
                                        <div class="rcg-issue-title">${safeHtml(issue.title || issue.code || 'Issue')}</div>
                                        <div class="rcg-issue-text">${safeHtml(issue.detail || '')}</div>
                                        <div class="rcg-muted-text" style="margin-top:5px;font-size:10px;">Affected: ${safeHtml(issue.affected_geometry || 'template')}</div>
                                        <div class="rcg-warning-text" style="margin-top:5px;font-size:10px;">Fix: ${safeHtml(issue.recommended_fix || '')}</div>
                                    </div>
                                </div>
                            </div>
                        `).join('') || '<div class="rcg-pane-subtitle" style="padding-top:12px;">No issues found. This template is ready for deployment review.</div>'}
                    </div>
                </div>
            `;
        } else {
            container.innerHTML = `
                <div class="rcg-inspector-scroll">
                    <div class="rcg-usage-card">
                        <div class="rcg-card-kicker">Usage</div>
                        <div class="rcg-card-title">${usage.length} linked action point${usage.length === 1 ? '' : 's'}</div>
                        <div class="rcg-pane-subtitle">Action points store a reference to the template only. Geometry stays centrally versioned here.</div>
                        ${template.status === 'published' && usage.length > 0 ? '<div class="rcg-warning-text" style="margin-top:10px;">This published template is in live use. Publishing edits will create a new safe version.</div>' : ''}
                    </div>
                    <div class="rcg-usage-card">
                        <div class="rcg-card-kicker">Linked Points</div>
                        ${usage.map((item) => `
                            <div class="rcg-usage-row">
                                <div>
                                    <div class="rcg-usage-title">${safeHtml(item.zone_name || 'Action Point')}</div>
                                    <div class="rcg-usage-text">
                                        ${safeHtml(item.action_id || 'No action selected')} ·
                                        ${safeHtml(item.point_type || 'generic')} ·
                                        ${item.recognize ? 'Shelf Detection On' : 'Shelf Detection Off'}
                                    </div>
                                </div>
                            </div>
                        `).join('') || '<div class="rcg-pane-subtitle" style="padding-top:12px;">No action points reference this template yet.</div>'}
                    </div>
                </div>
            `;
        }
    }

    function renderRecognition() {
        renderTopToolbar();
        renderLibrary();
        renderStageHeader();
        renderStage();
        renderInspector();
        bindToolbarButtons();
        refreshActionPointForms();
    }

    function bindToolbarButtons() {
        document.querySelectorAll('[data-rcg-category]').forEach((button) => {
            button.classList.toggle('active', String(button.getAttribute('data-rcg-category') || '') === state.activeCategory);
        });
        document.querySelectorAll('[data-rcg-inspector-tab]').forEach((button) => {
            button.classList.toggle('active', String(button.getAttribute('data-rcg-inspector-tab') || '') === state.inspectorTab);
        });
        document.querySelectorAll('[data-rcg-view]').forEach((button) => {
            button.classList.toggle('active', String(button.getAttribute('data-rcg-view') || '') === state.stageView);
        });
        document.querySelectorAll('[data-rcg-tool]').forEach((button) => {
            button.classList.toggle('active', String(button.getAttribute('data-rcg-tool') || '') === state.tool);
        });
    }

    function pointerDown(event) {
        const stage = getStageElement();
        if (!stage || !state.editorTemplate) return;
        const target = event.target && typeof event.target.closest === 'function'
            ? event.target.closest('[data-geo-kind]')
            : null;
        const world = snapWorldPoint(screenToWorld(event.clientX, event.clientY));
        stage.focus();

        if (state.tool === 'point' || state.tool === 'endpoint') {
            pushHistory();
            state.editorTemplate.geometry.points.push({
                id: generateEntityId('pt'),
                name: state.tool === 'endpoint' ? 'Endpoint' : 'Point',
                x: world.x,
                y: world.y,
                required: state.tool === 'endpoint',
                kind: state.tool === 'endpoint' ? 'endpoint' : 'anchor',
            });
            state.selectedGeometry = { kind: 'points', id: state.editorTemplate.geometry.points[state.editorTemplate.geometry.points.length - 1].id };
            markDirty();
            return;
        }

        if (state.tool === 'rectangle') {
            state.interaction = {
                mode: 'draw-rectangle',
                start: world,
                preview: { type: 'rectangle', start: world, end: world },
            };
            return;
        }

        if (state.tool === 'segment') {
            state.interaction = {
                mode: 'draw-segment',
                start: world,
                preview: { type: 'segment', start: world, end: world },
            };
            return;
        }

        if (state.tool === 'select') {
            if (target) {
                const kind = String(target.getAttribute('data-geo-kind') || '');
                const id = String(target.getAttribute('data-geo-id') || '');
                state.selectedGeometry = { kind, id };
                if (kind === 'points') {
                    const point = findSelection(kind, id);
                    state.interaction = { mode: 'drag-point', kind, id, origin: { x: Number(point && point.x || 0), y: Number(point && point.y || 0) } };
                } else if (kind === 'rectangles') {
                    const rect = findSelection(kind, id);
                    state.interaction = { mode: 'drag-rectangle', kind, id, origin: { x: Number(rect && rect.x || 0), y: Number(rect && rect.y || 0) }, anchor: world };
                } else if (kind === 'annotations') {
                    const annotation = findSelection(kind, id);
                    state.interaction = {
                        mode: 'drag-annotation',
                        kind,
                        id,
                        origin: clone(annotation),
                        anchor: world,
                    };
                } else {
                    state.interaction = null;
                }
                renderRecognition();
            } else {
                state.selectedGeometry = null;
                state.interaction = {
                    mode: 'pan',
                    startClient: { x: event.clientX, y: event.clientY },
                    origin: { x: Number(state.viewport.panX) || 0, y: Number(state.viewport.panY) || 0 },
                };
                renderRecognition();
            }
        }
    }

    function pointerMove(event) {
        if (!state.interaction || !state.editorTemplate) return;
        const world = snapWorldPoint(screenToWorld(event.clientX, event.clientY));
        if (state.interaction.mode === 'draw-rectangle' || state.interaction.mode === 'draw-segment') {
            state.interaction.preview = {
                type: state.interaction.mode === 'draw-rectangle' ? 'rectangle' : 'segment',
                start: state.interaction.start,
                end: world,
            };
            renderStage();
            return;
        }
        if (state.interaction.mode === 'drag-point') {
            const point = findSelection(state.interaction.kind, state.interaction.id);
            if (!point) return;
            point.x = world.x;
            point.y = world.y;
            state.editorTemplate.validation = validateTemplate(state.editorTemplate);
            renderRecognition();
            return;
        }
        if (state.interaction.mode === 'drag-rectangle') {
            const rect = findSelection(state.interaction.kind, state.interaction.id);
            if (!rect) return;
            const dx = world.x - state.interaction.anchor.x;
            const dy = world.y - state.interaction.anchor.y;
            rect.x = Number((state.interaction.origin.x + dx).toFixed(3));
            rect.y = Number((state.interaction.origin.y + dy).toFixed(3));
            state.editorTemplate.validation = validateTemplate(state.editorTemplate);
            renderRecognition();
            return;
        }
        if (state.interaction.mode === 'drag-annotation') {
            const annotation = findSelection(state.interaction.kind, state.interaction.id);
            if (!annotation) return;
            const dx = world.x - state.interaction.anchor.x;
            const dy = world.y - state.interaction.anchor.y;
            ['x1', 'x2'].forEach((key) => {
                annotation[key] = Number((Number(state.interaction.origin[key] || 0) + dx).toFixed(3));
            });
            ['y1', 'y2'].forEach((key) => {
                annotation[key] = Number((Number(state.interaction.origin[key] || 0) + dy).toFixed(3));
            });
            state.editorTemplate.validation = validateTemplate(state.editorTemplate);
            renderRecognition();
            return;
        }
        if (state.interaction.mode === 'pan') {
            const dx = event.clientX - state.interaction.startClient.x;
            const dy = event.clientY - state.interaction.startClient.y;
            state.viewport.panX = state.interaction.origin.x + dx;
            state.viewport.panY = state.interaction.origin.y + dy;
            renderRecognition();
        }
    }

    function pointerUp(event) {
        if (!state.interaction || !state.editorTemplate) return;
        const world = snapWorldPoint(screenToWorld(event.clientX, event.clientY));
        if (state.interaction.mode === 'draw-rectangle') {
            const width = Number((world.x - state.interaction.start.x).toFixed(3));
            const height = Number((world.y - state.interaction.start.y).toFixed(3));
            if (Math.abs(width) > 0.03 && Math.abs(height) > 0.03) {
                pushHistory();
                state.editorTemplate.geometry.rectangles.push({
                    id: generateEntityId('rect'),
                    role: 'capture_zone',
                    x: Math.min(state.interaction.start.x, world.x),
                    y: Math.min(state.interaction.start.y, world.y),
                    width: Math.abs(width),
                    height: Math.abs(height),
                });
                const latest = state.editorTemplate.geometry.rectangles[state.editorTemplate.geometry.rectangles.length - 1];
                state.selectedGeometry = { kind: 'rectangles', id: latest.id };
                if (!(Number(state.editorTemplate.dimensions.width) > 0)) state.editorTemplate.dimensions.width = Number(latest.width.toFixed(2));
                if (!(Number(state.editorTemplate.dimensions.depth) > 0)) state.editorTemplate.dimensions.depth = Number(latest.height.toFixed(2));
                markDirty();
            }
        } else if (state.interaction.mode === 'draw-segment') {
            if (Math.hypot(world.x - state.interaction.start.x, world.y - state.interaction.start.y) > 0.03) {
                pushHistory();
                const startId = generateEntityId('pt');
                const endId = generateEntityId('pt');
                state.editorTemplate.geometry.points.push(
                    { id: startId, name: 'Point A', x: state.interaction.start.x, y: state.interaction.start.y, required: false, kind: 'anchor' },
                    { id: endId, name: 'Point B', x: world.x, y: world.y, required: false, kind: 'anchor' },
                );
                const distance = Math.hypot(world.x - state.interaction.start.x, world.y - state.interaction.start.y);
                state.editorTemplate.geometry.segments.push({
                    id: generateEntityId('span'),
                    start: startId,
                    end: endId,
                    role: 'alignment',
                    distance: Number(distance.toFixed(3)),
                });
                const latest = state.editorTemplate.geometry.segments[state.editorTemplate.geometry.segments.length - 1];
                state.selectedGeometry = { kind: 'segments', id: latest.id };
                markDirty();
            }
        } else if (['drag-point', 'drag-rectangle', 'drag-annotation'].includes(state.interaction.mode)) {
            pushHistory();
            markDirty();
        }
        state.interaction = null;
        renderRecognition();
    }

    function handleStageWheel(event) {
        if (!state.editorTemplate) return;
        event.preventDefault();
        const nextZoom = Math.max(0.35, Math.min(2.8, Number(state.viewport.zoom) + (event.deltaY < 0 ? 0.08 : -0.08)));
        state.viewport.zoom = Number(nextZoom.toFixed(2));
        renderRecognition();
    }

    function bindDom() {
        if (state.domBound) return;
        state.domBound = true;
        const stage = getStageElement();
        if (stage) {
            stage.addEventListener('pointerdown', pointerDown);
            window.addEventListener('pointermove', pointerMove);
            window.addEventListener('pointerup', pointerUp);
            stage.addEventListener('wheel', handleStageWheel, { passive: false });
        }
        const importInput = document.getElementById('recognition-import-input');
        if (importInput) {
            importInput.addEventListener('change', async (event) => {
                const file = event.target && event.target.files ? event.target.files[0] : null;
                if (!file) return;
                try {
                    const text = await file.text();
                    const parsed = JSON.parse(text);
                    const rawTemplate = parsed && typeof parsed === 'object' && parsed.template ? parsed.template : parsed;
                    const imported = normalizeTemplate(rawTemplate);
                    imported.status = 'draft';
                    imported.template_id = imported.template_id.startsWith('local_') ? imported.template_id : `local_import_${Date.now()}`;
                    imported.family_key = imported.family_key.startsWith('local_') ? imported.family_key : `local_import_${Date.now()}`;
                    const merged = mergeTemplate(imported);
                    setActiveTemplate(merged);
                    state.dirty = true;
                    setToolbarStatus(`Imported ${merged.name}`);
                    renderRecognition();
                } catch (error) {
                    if (typeof window.showStatus === 'function') {
                        window.showStatus(`Import failed: ${error.message || error}`, 'error');
                    }
                } finally {
                    event.target.value = '';
                }
            });
        }
        document.addEventListener('keydown', (event) => {
            const recognitionTabActive = document.body && document.body.classList.contains('recognition-tab-active');
            if (!recognitionTabActive) return;
            const modifier = event.ctrlKey || event.metaKey;
            if (modifier && event.key.toLowerCase() === 'z') {
                event.preventDefault();
                if (event.shiftKey) {
                    window.recognitionRedo();
                } else {
                    window.recognitionUndo();
                }
                return;
            }
            if (modifier && event.key.toLowerCase() === 'y') {
                event.preventDefault();
                window.recognitionRedo();
                return;
            }
            if (modifier && event.key.toLowerCase() === 'c') {
                event.preventDefault();
                window.recognitionCopyGeometry();
                return;
            }
            if (modifier && event.key.toLowerCase() === 'v') {
                event.preventDefault();
                window.recognitionPasteGeometry();
                return;
            }
            if (event.key === 'Delete' || event.key === 'Backspace') {
                const activeTag = document.activeElement ? document.activeElement.tagName : '';
                if (['INPUT', 'TEXTAREA', 'SELECT'].includes(activeTag)) return;
                event.preventDefault();
                deleteSelection();
            }
        });
    }

    function buildActionPointSummary(templateId, pointType, recognize) {
        const normalizedType = normalizePointType(pointType);
        if (normalizedType !== 'shelf') {
            return '<div class="rcg-action-summary">Select <strong>Shelf</strong> point type to link a recognition template.</div>';
        }
        const template = state.templates.find((entry) => String(entry.template_id || '') === String(templateId || ''));
        if (!template) {
            return '<div class="rcg-action-summary"><strong>Template missing.</strong> Choose a saved shelf template before deployment.</div>';
        }
        const dims = template.dimensions || {};
        const warnings = [];
        if (template.status === 'draft') warnings.push('Draft template');
        if (template.status === 'deprecated') warnings.push('Deprecated template');
        return `
            <div class="rcg-action-summary">
                <div><strong>${safeHtml(template.name)}</strong> · ${safeHtml(template.geometry_type || 'profile')} · v${Number(template.version || 1)}</div>
                <div style="margin-top:6px;">Width ${Number(dims.width || 0).toFixed(2)}m · Depth ${Number(dims.depth || 0).toFixed(2)}m · Opening ${Number(dims.opening_width || 0).toFixed(2)}m</div>
                <div style="margin-top:6px;">${recognize ? 'Shelf Detection On' : 'Shelf Detection Off'}${warnings.length ? ` · ${safeHtml(warnings.join(', '))}` : ''}</div>
            </div>
        `;
    }

    function getActionPointZoneType(prefix) {
        if (prefix === 'zone') {
            return String(window.currentZoneType || 'normal').trim().toLowerCase();
        }
        const select = document.getElementById(`${prefix}-type`);
        return String(select && select.value || 'normal').trim().toLowerCase();
    }

    function refreshActionPointForm(prefix) {
        const pointTypeEl = document.getElementById(`${prefix}-point-type`);
        const templateEl = document.getElementById(`${prefix}-shelf-template`);
        const recognizeEl = document.getElementById(`${prefix}-shelf-recognize`);
        const groupEl = document.getElementById(`${prefix}-action-point-group`);
        const shelfConfigEl = document.getElementById(`${prefix}-shelf-config-group`);
        const summaryEl = document.getElementById(`${prefix}-shelf-summary`);
        const zoneType = getActionPointZoneType(prefix);
        const showActionPoint = zoneType === 'action';
        if (groupEl) groupEl.style.display = showActionPoint ? 'block' : 'none';
        if (!showActionPoint) {
            if (shelfConfigEl) shelfConfigEl.style.display = 'none';
            return;
        }
        const pointType = pointTypeEl ? normalizePointType(pointTypeEl.value || 'generic') : 'generic';
        const shelfTemplates = state.templates.filter((template) => normalizeCategory(template.category) === 'shelves');
        if (templateEl) {
            const selectedValue = String(templateEl.value || '').trim();
            const options = ['<option value="">Select shelf template</option>']
                .concat(shelfTemplates.map((template) => (
                    `<option value="${safeHtml(template.template_id)}"${selectedValue === template.template_id ? ' selected' : ''}>${safeHtml(template.name)} · v${Number(template.version || 1)} · ${safeHtml(template.status)}</option>`
                )));
            templateEl.innerHTML = options.join('');
        }
        if (shelfConfigEl) {
            shelfConfigEl.style.display = pointType === 'shelf' ? 'block' : 'none';
        }
        if (summaryEl) {
            summaryEl.innerHTML = buildActionPointSummary(templateEl ? templateEl.value : '', pointType, Boolean(recognizeEl && recognizeEl.checked));
        }
    }

    function getActionPointPayloadFromForm(prefix) {
        const zoneType = getActionPointZoneType(prefix);
        if (zoneType !== 'action') {
            return {
                point_type: 'generic',
                template_id: '',
                recognize: false,
            };
        }
        const pointTypeEl = document.getElementById(`${prefix}-point-type`);
        const templateEl = document.getElementById(`${prefix}-shelf-template`);
        const recognizeEl = document.getElementById(`${prefix}-shelf-recognize`);
        const payload = {
            point_type: pointTypeEl ? normalizePointType(pointTypeEl.value || 'generic') : 'generic',
            template_id: '',
            recognize: false,
        };
        if (payload.point_type === 'shelf') {
            payload.template_id = String(templateEl && templateEl.value || '').trim();
            payload.recognize = Boolean(recognizeEl && recognizeEl.checked);
        }
        return payload;
    }

    window.recognitionInit = function recognitionInit(forceReload) {
        bindDom();
        bindToolbarButtons();
        if (!state.initialized || forceReload) {
            loadRecognitionData({ preserveSelection: true });
        } else {
            renderRecognition();
        }
    };

    window.recognitionRender = renderRecognition;
    window.recognitionLoadData = loadRecognitionData;
    window.recognitionSelectTemplate = function recognitionSelectTemplate(templateId) {
        selectTemplate(templateId);
    };
    window.recognitionSetCategory = function recognitionSetCategory(category) {
        state.activeCategory = normalizeCategory(category);
        const next = pickTemplateForCategory(state.activeCategory);
        const currentCategory = state.editorTemplate ? normalizeCategory(state.editorTemplate.category) : '';
        if (next && (!state.editorTemplate || (!state.dirty && currentCategory !== state.activeCategory))) {
            setActiveTemplate(next);
        } else {
            renderRecognition();
        }
        bindToolbarButtons();
    };
    window.recognitionSetSearch = function recognitionSetSearch(value) {
        state.search = String(value || '').trim();
        renderRecognition();
    };
    window.recognitionCreateTemplate = function recognitionCreateTemplate(category) {
        createLocalTemplate(category || state.activeCategory);
        setToolbarStatus('New draft created');
    };
    window.recognitionDuplicateActive = async function recognitionDuplicateActive(optionalTemplateId) {
        const sourceId = String(optionalTemplateId || (state.editorTemplate && state.editorTemplate.template_id) || '').trim();
        if (!sourceId) {
            window.recognitionCreateTemplate();
            return;
        }
        const persisted = state.templates.find((template) => String(template.template_id || '') === sourceId);
        if (!persisted || state.usingDemoData) {
            const duplicate = normalizeTemplate(clone(persisted || state.editorTemplate), persisted || state.editorTemplate);
            duplicate.name = `${duplicate.name} Copy`;
            duplicate.status = 'draft';
            duplicate.version = 1;
            duplicate.family_key = `${slugify(duplicate.category)}_${slugify(duplicate.name)}`;
            duplicate.template_id = `local_${duplicate.family_key}_${Date.now()}`;
            duplicate.usage = [];
            duplicate.usage_count = 0;
            const merged = mergeTemplate(duplicate);
            setActiveTemplate(merged);
            state.dirty = true;
            setToolbarStatus('Duplicated draft locally');
            return;
        }
        try {
            const response = await fetch('/api/recognition/templates/duplicate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ template_id: sourceId }),
            });
            const data = await response.json();
            if (data.ok && data.template) {
                const merged = mergeTemplate(data.template);
                setActiveTemplate(merged);
                setToolbarStatus(data.message || 'Template duplicated');
            } else if (typeof window.showStatus === 'function') {
                window.showStatus(data.message || 'Failed to duplicate template', 'error');
            }
        } catch (error) {
            if (typeof window.showStatus === 'function') {
                window.showStatus(`Failed to duplicate template: ${error.message || error}`, 'error');
            }
        }
    };
    window.recognitionCopyGeometry = function recognitionCopyGeometry(optionalTemplateId) {
        const source = optionalTemplateId
            ? state.templates.find((template) => String(template.template_id || '') === String(optionalTemplateId || ''))
            : getActiveTemplate();
        if (!source) return;
        state.clipboardGeometry = {
            geometry: clone(source.geometry),
            dimensions: clone(source.dimensions),
            category: source.category,
        };
        setToolbarStatus(`Copied geometry from ${source.name}`);
    };
    window.recognitionPasteGeometry = function recognitionPasteGeometry() {
        if (!state.clipboardGeometry) {
            if (typeof window.showStatus === 'function') {
                window.showStatus('Copy a template geometry first', 'warning');
            }
            return;
        }
        const template = ensureActiveTemplate();
        pushHistory();
        template.geometry = clone(state.clipboardGeometry.geometry);
        template.dimensions = clone(state.clipboardGeometry.dimensions);
        if (state.clipboardGeometry.category) {
            template.category = normalizeCategory(state.clipboardGeometry.category);
        }
        markDirty();
        setToolbarStatus('Pasted geometry into current draft');
    };
    window.recognitionSaveDraft = async function recognitionSaveDraft() {
        const template = ensureActiveTemplate();
        const sourceTemplateId = String(template.template_id || '').trim();
        try {
            const response = await fetch('/api/recognition/templates/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ template }),
            });
            const data = await response.json();
            if (data.ok && data.template) {
                if (isLocalTemplate(template) && String(data.template.template_id || '').trim() !== sourceTemplateId) {
                    state.templates = state.templates.filter((item) => String(item.template_id || '').trim() !== sourceTemplateId);
                }
                const merged = mergeTemplate(data.template);
                setActiveTemplate(merged);
                setToolbarStatus(data.message || 'Draft saved');
                await loadRecognitionData({ preserveSelection: true });
                if (typeof window.showStatus === 'function') {
                    window.showStatus(data.message || 'Recognition draft saved', 'success');
                }
            } else if (typeof window.showStatus === 'function') {
                window.showStatus(data.message || 'Failed to save recognition draft', 'error');
            }
        } catch (error) {
            if (typeof window.showStatus === 'function') {
                window.showStatus(`Failed to save draft: ${error.message || error}`, 'error');
            }
        }
    };
    window.recognitionValidateActive = async function recognitionValidateActive() {
        const template = ensureActiveTemplate();
        try {
            const response = await fetch('/api/recognition/templates/validate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ template }),
            });
            const data = await response.json();
            if (data.template) {
                state.editorTemplate = normalizeTemplate(data.template, data.template);
            } else {
                state.editorTemplate.validation = validateTemplate(state.editorTemplate);
            }
            state.lastValidationMessage = String(data.message || '').trim();
            state.inspectorTab = 'validation';
            renderRecognition();
            bindToolbarButtons();
            setToolbarStatus(data.message || 'Validation complete');
            if (typeof window.showStatus === 'function') {
                window.showStatus(data.message || 'Validation complete', data.ok ? 'success' : 'warning');
            }
        } catch (error) {
            if (typeof window.showStatus === 'function') {
                window.showStatus(`Validation failed: ${error.message || error}`, 'error');
            }
        }
    };
    window.recognitionPublishActive = async function recognitionPublishActive() {
        const template = ensureActiveTemplate();
        const sourceTemplateId = String(template.template_id || '').trim();
        try {
            const response = await fetch('/api/recognition/templates/publish', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ template }),
            });
            const data = await response.json();
            if (data.ok && data.template) {
                if (isLocalTemplate(template) && String(data.template.template_id || '').trim() !== sourceTemplateId) {
                    state.templates = state.templates.filter((item) => String(item.template_id || '').trim() !== sourceTemplateId);
                }
                const merged = mergeTemplate(data.template);
                setActiveTemplate(merged);
                setToolbarStatus(data.message || 'Template published');
                await loadRecognitionData({ preserveSelection: true });
                if (typeof window.showStatus === 'function') {
                    window.showStatus(data.message || 'Template published', 'success');
                }
            } else {
                if (data.template) {
                    state.editorTemplate = normalizeTemplate(data.template, data.template);
                    state.inspectorTab = 'validation';
                    renderRecognition();
                }
                if (typeof window.showStatus === 'function') {
                    window.showStatus(data.message || 'Failed to publish template', 'error');
                }
            }
        } catch (error) {
            if (typeof window.showStatus === 'function') {
                window.showStatus(`Publish failed: ${error.message || error}`, 'error');
            }
        }
    };
    window.recognitionDeleteActive = async function recognitionDeleteActive() {
        const template = getActiveTemplate();
        if (!template) return;
        const confirmed = typeof window.showConfirm === 'function'
            ? await window.showConfirm(`Delete recognition template "${template.name}"?`, 'Delete', 'Cancel')
            : window.confirm(`Delete recognition template "${template.name}"?`);
        if (!confirmed) return;
        if (isLocalTemplate(template) || state.usingDemoData) {
            state.templates = state.templates.filter((item) => String(item.template_id || '') !== String(template.template_id || ''));
            state.editorTemplate = null;
            state.activeTemplateId = '';
            state.dirty = false;
            const next = pickTemplateForCategory(state.activeCategory) || state.templates[0];
            if (next) setActiveTemplate(next);
            else createLocalTemplate(state.activeCategory);
            setToolbarStatus('Local draft cleared');
            return;
        }
        try {
            const response = await fetch('/api/recognition/templates/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ template_id: template.template_id }),
            });
            const data = await response.json();
            if (data.ok) {
                state.templates = state.templates.filter((item) => String(item.template_id || '') !== String(template.template_id || ''));
                state.editorTemplate = null;
                state.activeTemplateId = '';
                state.dirty = false;
                await loadRecognitionData({ preserveSelection: false });
                setToolbarStatus('Template deleted');
                if (typeof window.showStatus === 'function') {
                    window.showStatus(data.message || 'Template deleted', 'success');
                }
            } else if (typeof window.showStatus === 'function') {
                window.showStatus(data.message || 'Failed to delete template', 'error');
            }
        } catch (error) {
            if (typeof window.showStatus === 'function') {
                window.showStatus(`Delete failed: ${error.message || error}`, 'error');
            }
        }
    };
    window.recognitionExportActive = async function recognitionExportActive() {
        const template = getActiveTemplate();
        if (!template) return;
        let exportPayload = { template };
        if (!isLocalTemplate(template) && !state.usingDemoData) {
            try {
                const response = await fetch(`/api/recognition/templates/export/${encodeURIComponent(template.template_id)}`);
                const data = await response.json();
                if (data.ok && data.template) {
                    exportPayload = { template: data.template };
                }
            } catch (_err) {
                exportPayload = { template };
            }
        }
        const blob = new Blob([JSON.stringify(exportPayload, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${slugify(template.name || template.template_id, 'recognition_template')}.json`;
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
        setToolbarStatus(`Exported ${template.name}`);
    };
    window.recognitionTriggerImport = function recognitionTriggerImport() {
        const input = document.getElementById('recognition-import-input');
        if (input) input.click();
    };
    window.recognitionSetInspectorTab = function recognitionSetInspectorTab(tab) {
        state.inspectorTab = ['properties', 'geometry', 'validation', 'usage'].includes(String(tab || '')) ? String(tab) : 'properties';
        renderRecognition();
        bindToolbarButtons();
    };
    window.recognitionSetTool = function recognitionSetTool(tool) {
        state.tool = ['select', 'point', 'endpoint', 'rectangle', 'segment'].includes(String(tool || '')) ? String(tool) : 'select';
        renderStageHeader();
        bindToolbarButtons();
        renderStage();
    };
    window.recognitionSetStageView = function recognitionSetStageView(view) {
        state.stageView = ['top', 'side', 'iso'].includes(String(view || '')) ? String(view) : 'top';
        bindToolbarButtons();
        renderStageHeader();
    };
    window.recognitionZoom = function recognitionZoom(delta) {
        state.viewport.zoom = Math.max(0.35, Math.min(2.8, Number(state.viewport.zoom || 1) + Number(delta || 0)));
        renderRecognition();
    };
    window.recognitionFitView = function recognitionFitView() {
        state.viewport = { ...DEFAULT_VIEW };
        renderRecognition();
    };
    window.recognitionToggleGrid = function recognitionToggleGrid() {
        state.viewport.showGrid = !state.viewport.showGrid;
        renderRecognition();
    };
    window.recognitionToggleLabels = function recognitionToggleLabels() {
        state.viewport.showLabels = !state.viewport.showLabels;
        renderRecognition();
    };
    window.recognitionToggleConstraints = function recognitionToggleConstraints() {
        state.viewport.showConstraints = !state.viewport.showConstraints;
        renderRecognition();
    };
    window.recognitionUndo = function recognitionUndo() {
        if (state.history.length <= 1) return;
        const current = state.history.pop();
        state.future.push(clone(current));
        const previous = state.history[state.history.length - 1];
        restoreFromHistory(previous);
    };
    window.recognitionRedo = function recognitionRedo() {
        if (!state.future.length) return;
        const next = state.future.pop();
        state.history.push(clone(next));
        restoreFromHistory(next);
    };
    window.recognitionDeleteSelection = deleteSelection;
    window.recognitionUpdateTemplateField = function recognitionUpdateTemplateField(path, rawValue) {
        const template = ensureActiveTemplate();
        pushHistory();
        const value = rawValue;
        if (path === 'name') {
            template.name = String(value || '').trim();
        } else if (path === 'category') {
            template.category = normalizeCategory(value);
        } else if (path === 'geometry_type') {
            template.geometry_type = String(value || '').trim().toLowerCase();
        } else if (path.startsWith('dimensions.')) {
            const key = path.split('.')[1];
            template.dimensions[key] = Number(value);
        } else if (path === 'notes') {
            template.notes = String(value || '');
        }
        markDirty();
    };
    window.recognitionUpdateSelectedGeometryField = function recognitionUpdateSelectedGeometryField(field, rawValue) {
        if (!state.selectedGeometry || !state.editorTemplate) return;
        const entity = findSelection(state.selectedGeometry.kind, state.selectedGeometry.id);
        if (!entity) return;
        pushHistory();
        if (['x', 'y', 'width', 'height', 'distance', 'x1', 'y1', 'x2', 'y2'].includes(field)) {
            entity[field] = Number(rawValue);
        } else if (field === 'required') {
            entity.required = Boolean(rawValue);
        } else {
            entity[field] = rawValue;
        }
        if (state.selectedGeometry.kind === 'segments') {
            entity.distance = Number(entity.distance || 0);
        }
        markDirty();
    };
    window.recognitionAddConstraint = function recognitionAddConstraint(type) {
        const template = ensureActiveTemplate();
        pushHistory();
        if (type === 'center_lock') {
            const targetPoint = state.selectedGeometry && state.selectedGeometry.kind === 'points'
                ? state.selectedGeometry.id
                : ((template.geometry.points || []).find((point) => point.kind === 'center') || {}).id;
            if (!targetPoint) {
                if (typeof window.showStatus === 'function') {
                    window.showStatus('Select a point or add a center point first', 'warning');
                }
                return;
            }
            template.constraints.push({
                id: generateEntityId('constraint'),
                type: 'center_lock',
                target: targetPoint,
                label: 'Center lock',
                required: true,
            });
        } else {
            let target = '';
            let value = 0;
            if (state.selectedGeometry && state.selectedGeometry.kind === 'segments') {
                target = state.selectedGeometry.id;
                const segment = findSelection('segments', target);
                value = Number(segment && segment.distance || 0);
            } else if ((template.geometry.segments || []).length > 0) {
                target = template.geometry.segments[0].id;
                value = Number(template.geometry.segments[0].distance || 0);
            }
            if (!target) {
                if (typeof window.showStatus === 'function') {
                    window.showStatus('Create or select a span first', 'warning');
                }
                return;
            }
            template.constraints.push({
                id: generateEntityId('constraint'),
                type: 'distance',
                target,
                value: Number(value.toFixed(3)),
                label: 'Measured span',
                required: true,
            });
        }
        markDirty();
    };
    window.recognitionRemoveConstraint = function recognitionRemoveConstraint(constraintId) {
        if (!state.editorTemplate) return;
        pushHistory();
        state.editorTemplate.constraints = (state.editorTemplate.constraints || []).filter((constraint) => String(constraint.id || '') !== String(constraintId || ''));
        markDirty();
    };
    window.recognitionToggleOverflowMenu = function recognitionToggleOverflowMenu(event) {
        const menu = document.getElementById('recognition-overflow-menu');
        const button = event && (event.currentTarget || event.target);
        if (!menu || !button) return;
        if (menu.classList.contains('open')) {
            menu.classList.remove('open');
            return;
        }
        const rect = button.getBoundingClientRect();
        menu.style.left = `${Math.min(rect.left, window.innerWidth - 240)}px`;
        menu.style.top = `${rect.bottom + 6}px`;
        menu.classList.add('open');
    };
    window.recognitionCloseMenus = function recognitionCloseMenus() {
        const menu = document.getElementById('recognition-overflow-menu');
        if (menu) menu.classList.remove('open');
    };
    window.recognitionGetActionPointPayloadFromForm = getActionPointPayloadFromForm;
    window.recognitionRefreshActionPointForm = refreshActionPointForm;
    window.recognitionHandleActionPointTypeChange = function recognitionHandleActionPointTypeChange(prefix) {
        refreshActionPointForm(prefix);
    };
    window.recognitionHandleActionPointTemplateChange = function recognitionHandleActionPointTemplateChange(prefix) {
        refreshActionPointForm(prefix);
    };

    document.addEventListener('click', (event) => {
        const menu = document.getElementById('recognition-overflow-menu');
        const button = document.getElementById('recognition-more-btn');
        if (!menu || !menu.classList.contains('open')) return;
        const insideMenu = menu.contains(event.target);
        const onButton = button && button.contains(event.target);
        if (!insideMenu && !onButton) {
            menu.classList.remove('open');
        }
    });

    document.addEventListener('DOMContentLoaded', () => {
        bindDom();
        if (document.getElementById('recognition-tab')) {
            renderRecognition();
            refreshActionPointForms();
            loadRecognitionData({ preserveSelection: true });
        }
    });
})();
