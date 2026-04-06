#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
from PIL import Image
from scipy import ndimage


OCCUPIED_THRESHOLD = 20
BACKGROUND_SHADE = 236
DOT_SHADE = 0


def density_clean(mask: np.ndarray) -> np.ndarray:
    n3 = ndimage.convolve(
        mask.astype(np.int16),
        np.ones((3, 3), dtype=np.int16),
        mode='constant',
        cval=0,
    )
    n5 = ndimage.convolve(
        mask.astype(np.int16),
        np.ones((5, 5), dtype=np.int16),
        mode='constant',
        cval=0,
    )
    keep = mask & ((n3 >= 3) | (n5 >= 6))

    structure = ndimage.generate_binary_structure(2, 2)
    labels, count = ndimage.label(keep, structure=structure)
    sizes = ndimage.sum(keep.astype(np.int16), labels, range(1, count + 1))

    out = np.zeros_like(keep, dtype=bool)
    for idx, size in enumerate(sizes, start=1):
        if int(size) >= 3:
            out[labels == idx] = True
    return out


def grid_quantized_dots(
    mask: np.ndarray,
    *,
    step: int = 3,
    min_occ_ratio: float = 0.05,
    thin_count: int = 1,
) -> np.ndarray:
    height, width = mask.shape
    cell_h = (height + step - 1) // step
    cell_w = (width + step - 1) // step
    out = np.zeros_like(mask, dtype=bool)

    occ_ratio = np.zeros((cell_h, cell_w), dtype=np.float32)
    occ_count = np.zeros((cell_h, cell_w), dtype=np.int16)

    for gy in range(cell_h):
        y0 = gy * step
        y1 = min(height, (gy + 1) * step)
        for gx in range(cell_w):
            x0 = gx * step
            x1 = min(width, (gx + 1) * step)
            cell = mask[y0:y1, x0:x1]
            occ_ratio[gy, gx] = float(cell.mean())
            occ_count[gy, gx] = int(cell.sum())

    strong = occ_ratio >= min_occ_ratio
    thin = occ_count >= thin_count

    neighbors = np.zeros_like(occ_count, dtype=np.int16)
    neighbors[:-1, :] += thin[1:, :]
    neighbors[1:, :] += thin[:-1, :]
    neighbors[:, :-1] += thin[:, 1:]
    neighbors[:, 1:] += thin[:, :-1]

    diagonal = np.zeros_like(occ_count, dtype=np.int16)
    diagonal[:-1, :-1] += thin[1:, 1:]
    diagonal[:-1, 1:] += thin[1:, :-1]
    diagonal[1:, :-1] += thin[:-1, 1:]
    diagonal[1:, 1:] += thin[:-1, :-1]

    keep_cells = strong | (thin & ((neighbors >= 1) | (diagonal >= 2)))

    for gy in range(cell_h):
        cy = min(height - 1, gy * step + step // 2)
        for gx in range(cell_w):
            if keep_cells[gy, gx]:
                cx = min(width - 1, gx * step + step // 2)
                out[cy, cx] = True

    return out


def render(dot_mask: np.ndarray) -> np.ndarray:
    vis = np.full(dot_mask.shape, BACKGROUND_SHADE, dtype=np.uint8)
    vis[dot_mask] = DOT_SHADE
    return vis


def generate_dotted_truth(
    raw: np.ndarray,
    *,
    occupied_threshold: int = OCCUPIED_THRESHOLD,
    step: int = 3,
    min_occ_ratio: float = 0.05,
    thin_count: int = 1,
) -> np.ndarray:
    occ = raw <= occupied_threshold
    clean_occ = density_clean(occ)
    dots = grid_quantized_dots(
        clean_occ,
        step=step,
        min_occ_ratio=min_occ_ratio,
        thin_count=thin_count,
    )
    return render(dots)


def convert_map(
    input_path: Path,
    output_path: Path,
    *,
    occupied_threshold: int = OCCUPIED_THRESHOLD,
    step: int = 3,
    min_occ_ratio: float = 0.05,
    thin_count: int = 1,
) -> None:
    raw = np.array(Image.open(input_path).convert('L'), dtype=np.uint8)
    out = generate_dotted_truth(
        raw,
        occupied_threshold=occupied_threshold,
        step=step,
        min_occ_ratio=min_occ_ratio,
        thin_count=thin_count,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(out).save(output_path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Generate a dotted truth PGM map.')
    parser.add_argument('--input', required=True, help='Input PGM path')
    parser.add_argument('--output', required=True, help='Output PGM path')
    parser.add_argument('--occupied-threshold', type=int, default=OCCUPIED_THRESHOLD)
    parser.add_argument('--step', type=int, default=3)
    parser.add_argument('--min-occ-ratio', type=float, default=0.05)
    parser.add_argument('--thin-count', type=int, default=1)
    return parser


def main() -> int:
    args = _parser().parse_args()
    convert_map(
        Path(args.input).expanduser().resolve(),
        Path(args.output).expanduser().resolve(),
        occupied_threshold=int(args.occupied_threshold),
        step=max(1, int(args.step)),
        min_occ_ratio=max(0.0, float(args.min_occ_ratio)),
        thin_count=max(1, int(args.thin_count)),
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
