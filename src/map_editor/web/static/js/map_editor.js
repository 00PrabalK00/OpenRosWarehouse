// Professional Map Editor - Interactive Canvas

const API_BASE = 'http://localhost:5001/api';

let mapInfo = null;
let baseMap = null;
let mapImage = null;
let currentTool = null;
let editingEnabled = false;
let drawingPoints = [];
let tempDrawing = null;
let layers = {
    obstacles: [],
    no_go_zones: [],
    slow_zones: [],
    restricted: []
};
let visibleLayers = {
    obstacles: true,
    no_go_zones: true,
    slow_zones: true,
    restricted: true
};

let dottedView = false;
let dotSpacing = 15;
let dotRadius = 2;
let dotMarker = 'circle';
let dotThin = true;

let canvasZoom = 1;
let stitchedMap = null;
let stitchTransform = { x: 0, y: 0, rotation: 0, scale: 1 };
let stitchState = 'idle';
let alignmentMode = null;
let regionMatching = { selection: null, lastScore: null };
let referencePointMatching = { pairs: [], pendingBasePoint: null, lastError: null };
let isDraggingStitchedMap = false;
let dragStartWorld = null;
let dragStartTransform = null;
let dragMoved = false;
let suppressNextClick = false;

const canvas = document.getElementById('map-canvas');
const ctx = canvas.getContext('2d');

async function init() {
    await loadMapInfo();
    await loadLayers();
    await loadMapPreview();
    await loadStitchState();
    setupEventListeners();
    updateEditingUI();
    updateStatus('Enable editing, then load a map to stitch or choose a drawing tool');
}

async function loadMapInfo() {
    try {
        const response = await fetch(`${API_BASE}/map/info`);
        mapInfo = await response.json();

        document.getElementById('resolution-info').textContent =
            `Resolution: ${mapInfo.resolution}m/px`;
        document.getElementById('map-size').textContent =
            `Map: ${mapInfo.width} x ${mapInfo.height}`;
    } catch (error) {
        console.error('Failed to load map info:', error);
        updateStatus('Error loading map info');
    }
}

async function loadLayers() {
    try {
        const response = await fetch(`${API_BASE}/layers`);
        layers = await response.json();
        updateObjectCount();
    } catch (error) {
        console.error('Failed to load layers:', error);
    }
}

function previewUrl() {
    if (!dottedView) return `${API_BASE}/map/preview`;
    return `${API_BASE}/map/preview?dotted=true&spacing=${dotSpacing}&dot_radius=${dotRadius}&marker=${dotMarker}&thin=${dotThin}`;
}

async function loadMapPreview() {
    try {
        const response = await fetch(previewUrl());
        const data = await response.json();
        if (!response.ok || data.error) {
            throw new Error(data.error || 'Failed to load map preview');
        }

        const img = await loadImageFromBase64(data.image);
        mapImage = img;
        baseMap = {
            image: img,
            width: img.width,
            height: img.height,
            resolution: Number(mapInfo?.resolution || 0.05),
            origin: Array.isArray(mapInfo?.origin) ? mapInfo.origin : [0, 0, 0]
        };
        canvas.width = img.width;
        canvas.height = img.height;
        applyCanvasZoom();
        redrawCanvas();
    } catch (error) {
        console.error('Failed to load map preview:', error);
        updateStatus('Error loading map');
    }
}

async function loadStitchState() {
    try {
        const response = await fetch(`${API_BASE}/stitch/state`);
        const data = await response.json();
        if (response.ok) {
            await applyStitchPayload(data);
        }
    } catch (error) {
        console.error('Failed to load stitch state:', error);
    }
}

function redrawCanvas() {
    if (!mapImage) return;

    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(mapImage, 0, 0);
    drawStitchedOverlay();
    drawReferencePairs();

    if (tempDrawing) {
        drawTempObject(tempDrawing);
    }
}

function drawStitchedOverlay() {
    if (!stitchedMap?.image || !baseMap) return;

    const matrix = getStitchedCanvasTransform();
    ctx.save();
    ctx.globalAlpha = 0.48;
    ctx.setTransform(matrix.a, matrix.b, matrix.c, matrix.d, matrix.e, matrix.f);
    ctx.drawImage(stitchedMap.image, 0, 0);
    ctx.restore();

    const corners = [
        stitchedPixelToBasePixel(0, 0),
        stitchedPixelToBasePixel(stitchedMap.width, 0),
        stitchedPixelToBasePixel(stitchedMap.width, stitchedMap.height),
        stitchedPixelToBasePixel(0, stitchedMap.height)
    ];
    ctx.save();
    ctx.strokeStyle = 'rgba(40, 167, 69, 0.95)';
    ctx.lineWidth = 2;
    ctx.setLineDash([8, 4]);
    ctx.beginPath();
    ctx.moveTo(corners[0].x, corners[0].y);
    corners.slice(1).forEach((pt) => ctx.lineTo(pt.x, pt.y));
    ctx.closePath();
    ctx.stroke();
    ctx.restore();
}

function drawReferencePairs() {
    if (!stitchedMap) return;

    ctx.save();
    ctx.lineWidth = 2;
    referencePointMatching.pairs.forEach((pair, index) => {
        const basePoint = worldToPixel(pair.base.x, pair.base.y);
        const stitchedWorld = applyStitchTransformWorld(pair.stitched);
        const stitchedPoint = worldToPixel(stitchedWorld.x, stitchedWorld.y);

        ctx.strokeStyle = 'rgba(255, 193, 7, 0.95)';
        ctx.fillStyle = 'rgba(255, 193, 7, 0.95)';
        ctx.beginPath();
        ctx.moveTo(basePoint.x, basePoint.y);
        ctx.lineTo(stitchedPoint.x, stitchedPoint.y);
        ctx.stroke();

        drawPointMarker(basePoint, '#0ea5e9', String(index + 1));
        drawPointMarker(stitchedPoint, '#f59e0b', String(index + 1));
    });

    if (referencePointMatching.pendingBasePoint) {
        drawPointMarker(
            worldToPixel(referencePointMatching.pendingBasePoint.x, referencePointMatching.pendingBasePoint.y),
            '#dc2626',
            'B'
        );
    }
    ctx.restore();
}

function drawPointMarker(point, color, label) {
    ctx.save();
    ctx.fillStyle = color;
    ctx.strokeStyle = '#ffffff';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(point.x, point.y, 6, 0, 2 * Math.PI);
    ctx.fill();
    ctx.stroke();
    if (label) {
        ctx.fillStyle = '#111827';
        ctx.font = '11px sans-serif';
        ctx.fillText(label, point.x + 8, point.y - 8);
    }
    ctx.restore();
}

function drawTempObject(obj) {
    ctx.save();

    if (obj.type === 'line') {
        ctx.strokeStyle = obj.color || '#000000';
        ctx.lineWidth = 2;
        ctx.setLineDash([6, 3]);
        ctx.beginPath();
        ctx.moveTo(obj.x1, obj.y1);
        ctx.lineTo(obj.x2, obj.y2);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = obj.color || '#000000';
        ctx.beginPath();
        ctx.arc(obj.x1, obj.y1, 4, 0, 2 * Math.PI);
        ctx.fill();
        ctx.beginPath();
        ctx.arc(obj.x2, obj.y2, 4, 0, 2 * Math.PI);
        ctx.fill();
    } else if (obj.type === 'rectangle') {
        ctx.strokeStyle = obj.color || '#ff0000';
        ctx.lineWidth = 3;
        ctx.setLineDash([5, 5]);
        ctx.strokeRect(obj.x1, obj.y1, obj.x2 - obj.x1, obj.y2 - obj.y1);
    } else if (obj.type === 'circle') {
        ctx.strokeStyle = obj.color || '#ff0000';
        ctx.lineWidth = 3;
        ctx.setLineDash([5, 5]);
        const radius = distance(obj.x1, obj.y1, obj.x2, obj.y2);
        ctx.beginPath();
        ctx.arc(obj.x1, obj.y1, radius, 0, 2 * Math.PI);
        ctx.stroke();
    } else if (obj.type === 'polygon' && obj.points && obj.points.length > 0) {
        ctx.strokeStyle = obj.color || '#ff0000';
        ctx.fillStyle = obj.color ? `${obj.color}40` : 'rgba(255, 0, 0, 0.2)';
        ctx.lineWidth = 3;
        ctx.setLineDash([5, 5]);
        ctx.beginPath();
        ctx.moveTo(obj.points[0].x, obj.points[0].y);
        for (let i = 1; i < obj.points.length; i += 1) {
            ctx.lineTo(obj.points[i].x, obj.points[i].y);
        }
        ctx.closePath();
        ctx.fill();
        ctx.stroke();
        obj.points.forEach((point) => {
            ctx.fillStyle = '#ff0000';
            ctx.fillRect(point.x - 3, point.y - 3, 6, 6);
        });
    }

    ctx.restore();
}

function pixelToWorld(px, py) {
    const wx = mapInfo.origin[0] + px * mapInfo.resolution;
    const wy = mapInfo.origin[1] + (mapInfo.height - py) * mapInfo.resolution;
    return { x: wx, y: wy };
}

function worldToPixel(wx, wy) {
    const px = (wx - mapInfo.origin[0]) / mapInfo.resolution;
    const py = mapInfo.height - (wy - mapInfo.origin[1]) / mapInfo.resolution;
    return { x: px, y: py };
}

function stitchedPixelToWorld(px, py) {
    const wx = stitchedMap.origin[0] + px * stitchedMap.resolution;
    const wy = stitchedMap.origin[1] + (stitchedMap.height - py) * stitchedMap.resolution;
    return { x: wx, y: wy };
}

function stitchedCenterWorld() {
    if (!stitchedMap) return { x: 0, y: 0 };
    return {
        x: stitchedMap.origin[0] + stitchedMap.width * stitchedMap.resolution * 0.5,
        y: stitchedMap.origin[1] + stitchedMap.height * stitchedMap.resolution * 0.5
    };
}

function applyStitchTransformWorld(point) {
    if (!stitchedMap) return { x: point.x, y: point.y };
    const center = stitchedCenterWorld();
    const radians = stitchTransform.rotation * Math.PI / 180.0;
    const cosTheta = Math.cos(radians);
    const sinTheta = Math.sin(radians);
    const dx = point.x - center.x;
    const dy = point.y - center.y;
    return {
        x: center.x + stitchTransform.x + stitchTransform.scale * (cosTheta * dx - sinTheta * dy),
        y: center.y + stitchTransform.y + stitchTransform.scale * (sinTheta * dx + cosTheta * dy)
    };
}

function invertStitchTransformWorld(point) {
    if (!stitchedMap) return { x: point.x, y: point.y };
    const center = stitchedCenterWorld();
    const radians = stitchTransform.rotation * Math.PI / 180.0;
    const cosTheta = Math.cos(radians);
    const sinTheta = Math.sin(radians);
    const dx = point.x - center.x - stitchTransform.x;
    const dy = point.y - center.y - stitchTransform.y;
    const safeScale = Math.abs(stitchTransform.scale) > 1e-9 ? stitchTransform.scale : 1;
    return {
        x: center.x + (cosTheta * dx + sinTheta * dy) / safeScale,
        y: center.y + (-sinTheta * dx + cosTheta * dy) / safeScale
    };
}

function stitchedPixelToBasePixel(px, py) {
    const stitchedWorld = stitchedPixelToWorld(px, py);
    const baseWorld = applyStitchTransformWorld(stitchedWorld);
    return worldToPixel(baseWorld.x, baseWorld.y);
}

function getStitchedCanvasTransform() {
    const p0 = stitchedPixelToBasePixel(0, 0);
    const p1 = stitchedPixelToBasePixel(1, 0);
    const p2 = stitchedPixelToBasePixel(0, 1);
    return {
        a: p1.x - p0.x,
        b: p1.y - p0.y,
        c: p2.x - p0.x,
        d: p2.y - p0.y,
        e: p0.x,
        f: p0.y
    };
}

function distance(x1, y1, x2, y2) {
    return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
}

function polygonArea(points) {
    let area = 0;
    for (let i = 0; i < points.length; i += 1) {
        const j = (i + 1) % points.length;
        area += points[i].x * points[j].y;
        area -= points[j].x * points[i].y;
    }
    return Math.abs(area / 2.0);
}

function setupEventListeners() {
    document.querySelectorAll('.tool-btn[data-tool]').forEach((btn) => {
        btn.addEventListener('click', () => {
            selectTool(btn.dataset.tool);
        });
    });

    document.getElementById('editing-enabled').addEventListener('change', (event) => {
        editingEnabled = event.target.checked;
        if (!editingEnabled && currentTool && currentTool.startsWith('stitch')) {
            currentTool = null;
        }
        updateEditingUI();
        updateInstructions();
    });

    document.getElementById('layer-obstacles').addEventListener('change', async (event) => {
        visibleLayers.obstacles = event.target.checked;
        await loadMapPreview();
    });
    document.getElementById('layer-nogo').addEventListener('change', async (event) => {
        visibleLayers.no_go_zones = event.target.checked;
        await loadMapPreview();
    });
    document.getElementById('layer-slow').addEventListener('change', async (event) => {
        visibleLayers.slow_zones = event.target.checked;
        await loadMapPreview();
    });
    document.getElementById('layer-restricted').addEventListener('change', async (event) => {
        visibleLayers.restricted = event.target.checked;
        await loadMapPreview();
    });

    const dottedCheckbox = document.getElementById('dotted-view');
    const dottedParams = document.getElementById('dotted-params');
    dottedCheckbox.addEventListener('change', async (event) => {
        dottedView = event.target.checked;
        dottedParams.style.display = dottedView ? 'block' : 'none';
        await loadMapPreview();
    });

    document.getElementById('dot-radius').addEventListener('change', async (event) => {
        dotRadius = parseInt(event.target.value, 10) || 2;
        if (dottedView) await loadMapPreview();
    });

    document.getElementById('dot-marker').addEventListener('change', async (event) => {
        dotMarker = event.target.value || 'circle';
        if (dottedView) await loadMapPreview();
    });

    document.getElementById('dot-preset').addEventListener('change', async (event) => {
        const preset = event.target.value;
        if (preset === 'quality') {
            dotSpacing = 10;
            dotRadius = 2;
            dotThin = true;
            dotMarker = 'circle';
        } else if (preset === 'speed') {
            dotSpacing = 18;
            dotRadius = 1;
            dotThin = false;
            dotMarker = 'plus';
        } else if (preset === 'balanced_plus') {
            dotSpacing = 13;
            dotRadius = 2;
            dotThin = true;
            dotMarker = 'plus';
        } else {
            dotSpacing = 15;
            dotRadius = 2;
            dotThin = true;
            dotMarker = 'circle';
        }
        document.getElementById('dot-radius').value = String(dotRadius);
        document.getElementById('dot-marker').value = dotMarker;
        if (dottedView) await loadMapPreview();
    });

    document.getElementById('btn-clear-obstacles').addEventListener('click', () => clearLayer('obstacles'));
    document.getElementById('btn-clear-nogo').addEventListener('click', () => clearLayer('no_go_zones'));
    document.getElementById('btn-clear-slow').addEventListener('click', () => clearLayer('slow_zones'));
    document.getElementById('btn-clear-all').addEventListener('click', clearAllLayers);
    document.getElementById('btn-export').addEventListener('click', exportMap);

    document.getElementById('btn-stitch-map').addEventListener('click', loadStitchedMap);
    document.getElementById('btn-confirm-stitch').addEventListener('click', confirmStitch);
    document.getElementById('btn-cancel-stitch').addEventListener('click', cancelStitch);
    document.getElementById('btn-apply-reference').addEventListener('click', applyReferenceMatching);
    document.getElementById('btn-clear-reference').addEventListener('click', clearReferencePairs);
    document.getElementById('btn-stitch-rotate-left').addEventListener('click', () => rotateStitchedOverlay(-2));
    document.getElementById('btn-stitch-rotate-right').addEventListener('click', () => rotateStitchedOverlay(2));
    document.getElementById('btn-zoom-in').addEventListener('click', () => setZoom(canvasZoom * 1.25));
    document.getElementById('btn-zoom-out').addEventListener('click', () => setZoom(canvasZoom / 1.25));
    document.getElementById('btn-zoom-reset').addEventListener('click', () => setZoom(1));

    canvas.addEventListener('mousedown', handleMouseDown);
    canvas.addEventListener('mousemove', handleMouseMove);
    canvas.addEventListener('mouseup', handleMouseUp);
    canvas.addEventListener('mouseleave', handleMouseUp);
    canvas.addEventListener('click', handleClick);
    canvas.addEventListener('contextmenu', handleRightClick);
}

function updateEditingUI() {
    const idsRequiringEditing = [
        'tool-obstacle-rect', 'tool-obstacle-circle', 'tool-obstacle-poly', 'tool-wall-line',
        'tool-nogo', 'tool-slow', 'tool-restricted',
        'btn-clear-obstacles', 'btn-clear-nogo', 'btn-clear-slow', 'btn-clear-all', 'btn-export',
        'btn-stitch-map', 'tool-stitch-move', 'tool-stitch-region', 'tool-stitch-reference',
        'btn-apply-reference', 'btn-clear-reference', 'btn-confirm-stitch', 'btn-cancel-stitch',
        'btn-stitch-rotate-left', 'btn-stitch-rotate-right'
    ];
    idsRequiringEditing.forEach((id) => {
        const element = document.getElementById(id);
        if (element) {
            element.disabled = !editingEnabled;
        }
    });
}

function handleMouseDown(event) {
    if (currentTool !== 'stitch-move' || !stitchedMap) {
        return;
    }
    const point = getCanvasPoint(event);
    dragStartWorld = pixelToWorld(point.px, point.py);
    dragStartTransform = { ...stitchTransform };
    isDraggingStitchedMap = true;
    dragMoved = false;
}

function handleMouseMove(event) {
    const point = getCanvasPoint(event);
    const world = pixelToWorld(point.px, point.py);
    document.getElementById('coords-display').textContent =
        `X: ${world.x.toFixed(2)}m Y: ${world.y.toFixed(2)}m`;

    if (isDraggingStitchedMap && stitchedMap && dragStartWorld && dragStartTransform) {
        const dx = world.x - dragStartWorld.x;
        const dy = world.y - dragStartWorld.y;
        stitchTransform = {
            ...dragStartTransform,
            x: dragStartTransform.x + dx,
            y: dragStartTransform.y + dy
        };
        dragMoved = true;
        stitchState = 'rough_placement';
        redrawCanvas();
        refreshStitchUI();
        return;
    }

    if (drawingPoints.length > 0) {
        if (currentTool === 'wall-line') {
            tempDrawing = {
                type: 'line',
                x1: drawingPoints[0].px,
                y1: drawingPoints[0].py,
                x2: point.px,
                y2: point.py,
                color: '#000000'
            };
        } else if (currentTool === 'stitch-region') {
            tempDrawing = {
                type: 'rectangle',
                x1: drawingPoints[0].px,
                y1: drawingPoints[0].py,
                x2: point.px,
                y2: point.py,
                color: '#22c55e'
            };
        } else if (currentTool?.includes('rect') || currentTool?.includes('nogo') ||
            currentTool?.includes('slow') || currentTool?.includes('restricted')) {
            tempDrawing = {
                type: 'rectangle',
                x1: drawingPoints[0].px,
                y1: drawingPoints[0].py,
                x2: point.px,
                y2: point.py,
                color: getToolColor()
            };
        } else if (currentTool?.includes('circle')) {
            tempDrawing = {
                type: 'circle',
                x1: drawingPoints[0].px,
                y1: drawingPoints[0].py,
                x2: point.px,
                y2: point.py,
                color: getToolColor()
            };
        } else if (currentTool?.includes('poly')) {
            tempDrawing = {
                type: 'polygon',
                points: [...drawingPoints.map((p) => ({ x: p.px, y: p.py })), { x: point.px, y: point.py }],
                color: getToolColor()
            };
        }
        redrawCanvas();
    }
}

async function handleMouseUp() {
    if (!isDraggingStitchedMap) {
        return;
    }
    isDraggingStitchedMap = false;
    dragStartWorld = null;
    dragStartTransform = null;
    if (dragMoved) {
        suppressNextClick = true;
        await persistStitchTransform();
        updateStatus('Rough placement updated. Confirm placement or refine alignment.');
    }
}

function handleClick(event) {
    if (suppressNextClick) {
        suppressNextClick = false;
        return;
    }

    if (!currentTool) {
        updateStatus('Please select a tool first');
        return;
    }

    const point = getCanvasPoint(event);
    const world = pixelToWorld(point.px, point.py);

    if (currentTool === 'measure') {
        handleMeasure(point.px, point.py, world);
        return;
    }
    if (currentTool === 'area') {
        handleAreaMeasure(point.px, point.py, world);
        return;
    }
    if (currentTool === 'stitch-region') {
        handleRegionSelection(point.px, point.py, world);
        return;
    }
    if (currentTool === 'stitch-reference') {
        handleReferenceClick(world);
        return;
    }
    if (currentTool === 'stitch-move') {
        if (!stitchedMap) {
            updateStatus('Load a stitched map before moving the overlay');
        }
        return;
    }
    if (currentTool.includes('poly')) {
        handlePolygonDraw(point.px, point.py, world);
        return;
    }
    handleShapeDraw(point.px, point.py, world);
}

function handleRightClick(event) {
    event.preventDefault();

    if (currentTool && currentTool.includes('poly') && drawingPoints.length >= 3) {
        finishPolygon();
    }
}

function handleShapeDraw(px, py, world) {
    if (!editingEnabled) {
        updateStatus('Enable editing before changing the base map');
        return;
    }

    if (drawingPoints.length === 0) {
        drawingPoints.push({ px, py, world });
        updateStatus('Click again to finish shape');
    } else {
        const start = drawingPoints[0];
        const layer = getLayerForTool();

        let obj = {};
        if (currentTool === 'wall-line') {
            obj = { type: 'line', x1: start.world.x, y1: start.world.y, x2: world.x, y2: world.y };
        } else if (currentTool.includes('rect') || currentTool.includes('nogo') ||
            currentTool.includes('slow') || currentTool.includes('restricted')) {
            obj = {
                type: 'rectangle',
                x1: Math.min(start.world.x, world.x),
                y1: Math.min(start.world.y, world.y),
                x2: Math.max(start.world.x, world.x),
                y2: Math.max(start.world.y, world.y)
            };

            if (currentTool === 'slow') {
                obj.speed_limit = parseFloat(document.getElementById('speed-limit').value);
            }
        } else if (currentTool.includes('circle')) {
            obj = {
                type: 'circle',
                x: start.world.x,
                y: start.world.y,
                radius: distance(start.world.x, start.world.y, world.x, world.y)
            };
        }

        addLayerObject(layer, obj);
        drawingPoints = [];
        tempDrawing = null;
        updateStatus('Shape added');
    }
}

function handlePolygonDraw(px, py, world) {
    drawingPoints.push({ px, py, world });
    updateStatus(`Polygon: ${drawingPoints.length} points (right-click to finish)`);
}

async function finishPolygon() {
    if (drawingPoints.length < 3) return;

    const layer = getLayerForTool();
    const obj = {
        type: 'polygon',
        points: drawingPoints.map((point) => ({ x: point.world.x, y: point.world.y }))
    };

    await addLayerObject(layer, obj);
    drawingPoints = [];
    tempDrawing = null;
    updateStatus('Polygon added');
}

function handleMeasure(px, py, world) {
    if (drawingPoints.length === 0) {
        drawingPoints.push({ px, py, world });
        updateStatus('Click second point');
    } else {
        const start = drawingPoints[0];
        const dist = distance(start.world.x, start.world.y, world.x, world.y);
        document.getElementById('measurement-result').textContent = `Distance: ${dist.toFixed(3)}m`;
        drawingPoints = [];
        updateStatus('Measurement complete');
    }
}

function handleAreaMeasure(px, py, world) {
    drawingPoints.push({ px, py, world });
    if (drawingPoints.length < 3) {
        updateStatus(`Area: ${drawingPoints.length} points (right-click to finish)`);
        return;
    }
    const area = polygonArea(drawingPoints.map((point) => point.world));
    document.getElementById('measurement-result').textContent = `Area: ${area.toFixed(3)}m²`;
    updateStatus('Area updated. Right-click to finish or keep adding points.');
}

async function handleRegionSelection(px, py, world) {
    if (!stitchedMap) {
        updateStatus('Load a stitched map before running region matching');
        return;
    }

    if (drawingPoints.length === 0) {
        drawingPoints.push({ px, py, world });
        updateStatus('Drag or click the opposite corner of the shared-feature region');
        return;
    }

    const start = drawingPoints[0];
    drawingPoints = [];
    tempDrawing = null;
    regionMatching.selection = {
        x1: Math.min(start.world.x, world.x),
        y1: Math.min(start.world.y, world.y),
        x2: Math.max(start.world.x, world.x),
        y2: Math.max(start.world.y, world.y)
    };
    redrawCanvas();
    await applyRegionMatching();
}

function handleReferenceClick(world) {
    if (!stitchedMap) {
        updateStatus('Load a stitched map before selecting reference points');
        return;
    }
    if (!referencePointMatching.pendingBasePoint) {
        referencePointMatching.pendingBasePoint = { x: world.x, y: world.y };
        updateStatus('Base point saved. Click the matching point on the stitched overlay.');
    } else {
        const stitchedLocal = invertStitchTransformWorld(world);
        referencePointMatching.pairs.push({
            base: { ...referencePointMatching.pendingBasePoint },
            stitched: stitchedLocal
        });
        referencePointMatching.pendingBasePoint = null;
        updateStatus(`Reference pair ${referencePointMatching.pairs.length} captured. Add more or apply the match.`);
    }
    refreshStitchUI();
    redrawCanvas();
}

function getLayerForTool() {
    if (currentTool === 'wall-line') return 'obstacles';
    if (currentTool.includes('obstacle')) return 'obstacles';
    if (currentTool.includes('nogo')) return 'no_go_zones';
    if (currentTool.includes('slow')) return 'slow_zones';
    if (currentTool.includes('restricted')) return 'restricted';
    return 'obstacles';
}

function getToolColor() {
    if (currentTool.includes('obstacle')) return '#000000';
    if (currentTool.includes('nogo')) return '#ff0000';
    if (currentTool.includes('slow')) return '#ffff00';
    if (currentTool.includes('restricted')) return '#ffa500';
    if (currentTool === 'stitch-region') return '#22c55e';
    return '#000000';
}

async function addLayerObject(layer, obj) {
    try {
        const response = await fetch(`${API_BASE}/layer/add`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ layer, object: obj })
        });

        if (response.ok) {
            await loadLayers();
            await loadMapPreview();
            updateObjectCount();
        }
    } catch (error) {
        console.error('Failed to add object:', error);
        updateStatus('Error adding object');
    }
}

async function clearLayer(layer) {
    if (!confirm(`Clear all ${layer.replace('_', ' ')}?`)) return;

    try {
        const response = await fetch(`${API_BASE}/layer/clear`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ layer })
        });

        if (response.ok) {
            await loadLayers();
            await loadMapPreview();
            updateObjectCount();
            updateStatus(`Cleared ${layer}`);
        }
    } catch (error) {
        console.error('Failed to clear layer:', error);
    }
}

async function clearAllLayers() {
    if (!confirm('Clear ALL layers? This cannot be undone!')) return;

    for (const layer of Object.keys(layers)) {
        await clearLayer(layer);
    }
}

async function exportMap() {
    const defaultName = `edited_map_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}`;
    const baseName = prompt('Enter map name (without extension):', defaultName);
    if (!baseName) return;

    const pgmFilename = baseName.toLowerCase().endsWith('.pgm') ? baseName : `${baseName}.pgm`;

    try {
        updateStatus('Exporting map...');
        const response = await fetch(`${API_BASE}/map/export`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path: pgmFilename })
        });

        if (response.ok) {
            updateStatus(`Map exported to ${pgmFilename}`);
            alert(`Map successfully exported:\n${pgmFilename}\n${pgmFilename.replace(/\.pgm$/, '.yaml')}`);
        } else {
            updateStatus('Export failed');
        }
    } catch (error) {
        console.error('Failed to export:', error);
        updateStatus('Export error');
    }
}

async function loadStitchedMap() {
    if (!editingEnabled) {
        updateStatus('Enable editing before loading a stitched map');
        return;
    }
    const input = document.getElementById('stitch-map-path');
    const path = String(input.value || '').trim();
    if (!path) {
        updateStatus('Enter a stitched map YAML or image path');
        return;
    }

    try {
        updateStatus('Loading stitched map as a temporary overlay...');
        const payload = await postJson('/stitch/load', { path });
        referencePointMatching = { pairs: [], pendingBasePoint: null, lastError: null };
        regionMatching = { selection: null, lastScore: null };
        await applyStitchPayload(payload);
        selectTool('stitch-move');
        updateStatus('Stitched map loaded. Drag it into rough position, then confirm placement.');
    } catch (error) {
        console.error('Failed to load stitched map:', error);
        updateStatus(error.message);
    }
}

async function persistStitchTransform() {
    if (!stitchedMap) return;
    try {
        const payload = await postJson('/stitch/transform', { stitchTransform });
        await applyStitchPayload(payload, { keepLocalPairs: true, keepSelection: true });
    } catch (error) {
        console.error('Failed to persist stitch transform:', error);
        updateStatus(error.message);
    }
}

async function rotateStitchedOverlay(deltaDeg) {
    if (!stitchedMap) {
        updateStatus('Load a stitched map before rotating it');
        return;
    }
    stitchTransform.rotation += deltaDeg;
    stitchState = 'rough_placement';
    redrawCanvas();
    refreshStitchUI();
    await persistStitchTransform();
}

async function applyRegionMatching() {
    if (!stitchedMap) {
        updateStatus('Load a stitched map before region matching');
        return;
    }
    if (!regionMatching.selection) {
        updateStatus('Draw a valid rectangle over shared map features first');
        return;
    }
    try {
        updateStatus('Refining rotation and translation inside the selected region...');
        const payload = await postJson('/stitch/align/region', {
            selection: regionMatching.selection
        });
        await applyStitchPayload(payload, { keepLocalPairs: true, keepSelection: true });
        updateStatus('Region matching applied. You can repeat it to improve the alignment.');
    } catch (error) {
        console.error('Region matching failed:', error);
        updateStatus(error.message);
    }
}

async function applyReferenceMatching() {
    if (!stitchedMap) {
        updateStatus('Load a stitched map before reference point matching');
        return;
    }
    if (referencePointMatching.pairs.length < 2) {
        updateStatus('Reference point matching requires at least 2 valid pairs. 3 or more are recommended.');
        return;
    }
    try {
        updateStatus('Solving rotation, translation, and scale from reference points...');
        const payload = await postJson('/stitch/align/reference', {
            pairs: referencePointMatching.pairs,
            allowScale: true
        });
        await applyStitchPayload(payload, { keepLocalPairs: true, keepSelection: true });
        updateStatus('Reference point matching applied. Zoom in and refine with more pairs if needed.');
    } catch (error) {
        console.error('Reference point matching failed:', error);
        updateStatus(error.message);
    }
}

function clearReferencePairs() {
    referencePointMatching = { pairs: [], pendingBasePoint: null, lastError: null };
    refreshStitchUI();
    redrawCanvas();
    updateStatus('Reference pairs cleared');
}

async function confirmStitch() {
    if (!stitchedMap) {
        updateStatus('Load a stitched map before confirming');
        return;
    }
    try {
        if (stitchState === 'stitching_map_loaded' || stitchState === 'rough_placement') {
            const payload = await postJson('/stitch/confirm-placement', {});
            await applyStitchPayload(payload, { keepLocalPairs: true, keepSelection: true });
            updateStatus('Rough placement confirmed. Choose Region Matching or Reference Points to refine it.');
            return;
        }

        updateStatus('Merging the stitched map into the base map...');
        const payload = await postJson('/stitch/confirm', {});
        await loadMapPreview();
        await applyStitchPayload(payload);
        clearReferencePairs();
        regionMatching = { selection: null, lastScore: null };
        selectTool(null);
        updateStatus('Stitch confirmed. The base map is now updated with the merged result.');
    } catch (error) {
        console.error('Confirm stitch failed:', error);
        updateStatus(error.message);
    }
}

async function cancelStitch() {
    try {
        const payload = await postJson('/stitch/cancel', {});
        await applyStitchPayload(payload);
        clearReferencePairs();
        regionMatching = { selection: null, lastScore: null };
        drawingPoints = [];
        tempDrawing = null;
        redrawCanvas();
        updateStatus('Active stitching state cleared. Base map left unchanged.');
    } catch (error) {
        console.error('Cancel stitch failed:', error);
        updateStatus(error.message);
    }
}

async function applyStitchPayload(payload, options = {}) {
    const preserveImage = options.preserveImage !== false;
    const keepLocalPairs = options.keepLocalPairs === true;
    const keepSelection = options.keepSelection === true;

    stitchState = payload.state || 'idle';
    alignmentMode = payload.alignmentMode || null;
    stitchTransform = payload.stitchTransform || { x: 0, y: 0, rotation: 0, scale: 1 };
    regionMatching = keepSelection
        ? { ...regionMatching, ...(payload.regionMatching || {}) }
        : (payload.regionMatching || { selection: null, lastScore: null });

    if (!keepLocalPairs) {
        referencePointMatching = {
            pairs: [],
            pendingBasePoint: null,
            lastError: payload.referencePointMatching?.lastError ?? null
        };
    } else {
        referencePointMatching = {
            ...referencePointMatching,
            lastError: payload.referencePointMatching?.lastError ?? referencePointMatching.lastError
        };
    }

    if (payload.stitchedMap === null) {
        stitchedMap = null;
    } else if (payload.stitchedMap) {
        const nextStitchedMap = { ...(stitchedMap || {}), ...payload.stitchedMap };
        if (payload.stitchedMap.image) {
            nextStitchedMap.image = await loadImageFromBase64(payload.stitchedMap.image);
        } else if (preserveImage && stitchedMap?.image) {
            nextStitchedMap.image = stitchedMap.image;
        }
        stitchedMap = nextStitchedMap;
    }

    refreshStitchUI();
    redrawCanvas();
}

function refreshStitchUI() {
    document.getElementById('stitch-state-label').textContent = `State: ${stitchState}`;
    document.getElementById('stitch-mode-label').textContent = `Alignment: ${alignmentMode || '--'}`;
    document.getElementById('stitch-pairs-label').textContent = `Pairs: ${referencePointMatching.pairs.length}`;

    const confirmButton = document.getElementById('btn-confirm-stitch');
    if (stitchState === 'stitching_map_loaded' || stitchState === 'rough_placement') {
        confirmButton.textContent = 'Confirm Placement';
    } else {
        confirmButton.textContent = 'Confirm Stitch';
    }
    updateInstructions();
}

function selectTool(toolName) {
    if (toolName && toolName.startsWith('stitch') && !editingEnabled) {
        updateStatus('Enable editing before starting a stitch workflow');
        return;
    }
    document.querySelectorAll('.tool-btn[data-tool]').forEach((button) => {
        button.classList.toggle('active', button.dataset.tool === toolName);
    });
    currentTool = toolName;
    drawingPoints = [];
    tempDrawing = null;
    updateInstructions();
    redrawCanvas();
}

function updateInstructions() {
    const instructions = document.getElementById('instructions');
    if (!currentTool) {
        instructions.textContent = editingEnabled
            ? 'Choose a drawing tool or load a stitched map to begin'
            : 'Enable editing to modify the base map or stitch a second map';
        return;
    }

    const messages = {
        'obstacle-rect': 'Click two points to draw an obstacle rectangle on the base map',
        'obstacle-circle': 'Click the center, then click again to set the obstacle radius',
        'obstacle-poly': 'Click to add obstacle vertices. Right-click to finish the polygon.',
        'wall-line': 'Click a start point, then an end point to draw a wall line',
        'nogo': 'Click two corners to create a no-go zone',
        'slow': 'Click two corners to create a slow zone',
        'restricted': 'Click two corners to create a restricted zone',
        'measure': 'Click two points to measure the distance',
        'area': 'Click points to measure area. Right-click to finish.',
        'stitch-move': 'Drag the stitched overlay into rough position. Use the rotate buttons if needed.',
        'stitch-region': 'Draw a rectangle over shared features. Region matching will refine rotation and translation only.',
        'stitch-reference': referencePointMatching.pendingBasePoint
            ? 'Click the matching point on the stitched overlay'
            : 'Click a base-map point, then its matching stitched-map point'
    };
    instructions.textContent = messages[currentTool] || 'Draw on the map';
}

function updateStatus(text) {
    document.getElementById('status-text').textContent = text;
}

function updateObjectCount() {
    const total = Object.values(layers).reduce((sum, arr) => sum + arr.length, 0);
    document.getElementById('object-count').textContent = `Objects: ${total}`;
}

function setZoom(nextZoom) {
    canvasZoom = Math.max(0.5, Math.min(4.0, nextZoom));
    applyCanvasZoom();
}

function applyCanvasZoom() {
    canvas.style.width = `${canvas.width * canvasZoom}px`;
    canvas.style.height = `${canvas.height * canvasZoom}px`;
    document.getElementById('zoom-label').textContent = `${Math.round(canvasZoom * 100)}%`;
}

function getCanvasPoint(event) {
    const rect = canvas.getBoundingClientRect();
    const scaleX = canvas.width / rect.width;
    const scaleY = canvas.height / rect.height;
    return {
        px: (event.clientX - rect.left) * scaleX,
        py: (event.clientY - rect.top) * scaleY
    };
}

async function postJson(path, body) {
    const response = await fetch(`${API_BASE}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    const payload = await response.json();
    if (!response.ok || payload.error) {
        throw new Error(payload.error || `Request failed: ${path}`);
    }
    return payload;
}

function loadImageFromBase64(base64Data) {
    return new Promise((resolve, reject) => {
        const image = new Image();
        image.onload = () => resolve(image);
        image.onerror = () => reject(new Error('Failed to decode image data'));
        image.src = `data:image/png;base64,${base64Data}`;
    });
}

init();
