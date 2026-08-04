#!/usr/bin/env python3
"""Generate the app icon.

``icon.png`` is committed so the running app always has an icon. The platform
formats (.ico, .icns) are derived at build time — .icns needs macOS tooling, so
it cannot be produced anywhere else anyway.

    python packaging/make_icons.py            # (re)draw icon.png
    python packaging/make_icons.py --platform # also emit icon.ico / icon.icns
"""

import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESOURCES = PROJECT_ROOT / "app" / "desktop" / "resources"
PNG = RESOURCES / "icon.png"
SIZE = 1024

ACCENT_TOP = "#3f7ef5"
ACCENT_BOTTOM = "#1f4fbf"


def draw_png() -> Path:
    from PySide6.QtCore import QPointF, QRectF, Qt
    from PySide6.QtGui import QBrush, QColor, QGuiApplication, QImage, QLinearGradient, QPainter, QPen

    QGuiApplication.instance() or QGuiApplication([])

    image = QImage(SIZE, SIZE, QImage.Format_ARGB32)
    image.fill(Qt.transparent)

    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing)

    gradient = QLinearGradient(0, 0, 0, SIZE)
    gradient.setColorAt(0, QColor(ACCENT_TOP))
    gradient.setColorAt(1, QColor(ACCENT_BOTTOM))
    painter.setBrush(QBrush(gradient))
    painter.setPen(Qt.NoPen)
    painter.drawRoundedRect(QRectF(0, 0, SIZE, SIZE), SIZE * 0.22, SIZE * 0.22)

    # A stack of listing rows...
    painter.setBrush(QColor(255, 255, 255, 235))
    row_x, row_width, row_height = SIZE * 0.20, SIZE * 0.44, SIZE * 0.065
    for index in range(4):
        width = row_width if index % 2 == 0 else row_width * 0.72
        top = SIZE * 0.24 + index * SIZE * 0.125
        painter.drawRoundedRect(QRectF(row_x, top, width, row_height), row_height / 2, row_height / 2)

    # ...under a magnifier.
    painter.setBrush(Qt.NoBrush)
    pen = QPen(QColor("#ffffff"))
    pen.setWidthF(SIZE * 0.062)
    pen.setCapStyle(Qt.RoundCap)
    painter.setPen(pen)
    centre, radius = QPointF(SIZE * 0.63, SIZE * 0.60), SIZE * 0.19
    painter.drawEllipse(centre, radius, radius)
    painter.drawLine(
        QPointF(centre.x() + radius * 0.72, centre.y() + radius * 0.72),
        QPointF(SIZE * 0.86, SIZE * 0.83),
    )

    painter.end()
    RESOURCES.mkdir(parents=True, exist_ok=True)
    image.save(str(PNG), "PNG")
    return PNG


def make_ico() -> Path | None:
    try:
        from PIL import Image
    except ImportError:
        print("Pillow not installed; skipping icon.ico", file=sys.stderr)
        return None

    target = RESOURCES / "icon.ico"
    Image.open(PNG).save(target, sizes=[(s, s) for s in (16, 24, 32, 48, 64, 128, 256)])
    return target


def make_icns() -> Path | None:
    if sys.platform != "darwin":
        print("icon.icns can only be built on macOS; skipping", file=sys.stderr)
        return None

    target = RESOURCES / "icon.icns"
    with tempfile.TemporaryDirectory() as tmp:
        iconset = Path(tmp) / "icon.iconset"
        iconset.mkdir()
        for size in (16, 32, 64, 128, 256, 512):
            for scale, suffix in ((1, ""), (2, "@2x")):
                pixels = size * scale
                subprocess.run(
                    ["sips", "-z", str(pixels), str(pixels), str(PNG),
                     "--out", str(iconset / f"icon_{size}x{size}{suffix}.png")],
                    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
        subprocess.run(["iconutil", "-c", "icns", str(iconset), "-o", str(target)], check=True)
    return target


if __name__ == "__main__":
    print("wrote", draw_png())
    if "--platform" in sys.argv:
        for path in (make_ico(), make_icns()):
            if path:
                print("wrote", path)
