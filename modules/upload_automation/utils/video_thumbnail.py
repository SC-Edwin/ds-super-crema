"""비디오에서 대표 프레임(썸네일) 추출 — 플랫폼 비종속."""

from __future__ import annotations

import logging
import tempfile

try:
    import cv2

    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False

logger = logging.getLogger(__name__)


def extract_thumbnail_from_video(video_path: str, output_path: str | None = None) -> str:
    """
    OpenCV로 비디오 중간(또는 첫) 프레임을 JPEG로 저장하고 경로를 반환합니다.
    """
    if not CV2_AVAILABLE:
        raise RuntimeError(
            "opencv-python-headless is required for thumbnail extraction. "
            "Install it with: pip install opencv-python-headless"
        )

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")

    try:
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames > 0:
            frame_number = max(0, total_frames // 2)
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)

        ret, frame = cap.read()

        if not ret:
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ret, frame = cap.read()

        if not ret:
            raise RuntimeError(f"Cannot read frame from video: {video_path}")

        if output_path is None:
            output_path = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False).name

        cv2.imwrite(output_path, frame)
        logger.info("Extracted thumbnail from %s to %s", video_path, output_path)
        return output_path
    finally:
        cap.release()
