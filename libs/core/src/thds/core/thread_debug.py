import os
import re
import sys
import threading
import types
import typing as ty

from thds.core import log

logger = log.getLogger(__name__)


def _format_frame(frame: types.FrameType) -> str:
    """Format a stack frame as a single readable line."""
    return f"{frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}"


def _get_thread_stack_frames(thread: threading.Thread) -> list[str]:
    """Get stack frames for a specific thread."""
    frames = list[str]()

    if thread.ident is None:
        logger.warning('Thread "%s" has no identifier; cannot get stack frames.', thread.name)
        return frames

    # Get the frame for this thread
    current_frames = sys._current_frames()
    frame = current_frames.get(thread.ident)

    if frame is None:
        logger.warning("unable to get stack frames for thread %s (id: %s)")
        return frames

    # Get the path of this debug module
    debug_module_path = os.path.abspath(__file__)

    # Walk the stack
    while frame:
        if not (debug_module_path and os.path.abspath(frame.f_code.co_filename) == debug_module_path):

            frames.append(_format_frame(frame))
        frame = frame.f_back
    return frames


def _get_thread_info(thread: threading.Thread) -> dict[str, ty.Any]:
    """Extract thread information including stack trace."""
    stack_frames = _get_thread_stack_frames(thread)
    return {
        "thread_id": thread.ident,
        "thread_name": thread.name,
        "stack_frames": stack_frames,
    }


def _is_thread_pool_thread(thread: threading.Thread) -> bool:
    """Check if thread appears to belong to a ThreadPoolExecutor."""
    tpe_patterns = [
        r"ThreadPoolExecutor-\d+_\d+",  # Default naming
        r".*-\d+_\d+",  # Custom prefix with TPE suffix pattern
    ]

    for pattern in tpe_patterns:
        if re.match(pattern, thread.name):
            return True

    return False


def _find_potential_parent_threads() -> list[dict[str, ty.Any]]:
    """Find threads that could be parents of the current thread pool thread."""
    potential_parents = []
    current_thread = threading.current_thread()

    for thread in threading.enumerate():
        # Skip self
        if thread == current_thread:
            continue

        # Skip daemon threads
        if thread.daemon:
            continue

        # Skip other thread pool threads
        if _is_thread_pool_thread(thread):
            continue

        thread_info = _get_thread_info(thread)
        potential_parents.append(thread_info)

    return potential_parents


def capture_thread_context() -> dict[str, ty.Any]:
    """
    Capture stack trace for current thread and potential parent threads.

    Returns:
        Dict with 'current_thread' info and 'potential_parents' list.
    """
    current_thread = threading.current_thread()

    # Always capture current thread info
    current_info = _get_thread_info(current_thread)

    # Only look for parents if we think we're in a thread pool
    potential_parents = []
    if _is_thread_pool_thread(current_thread):
        potential_parents = _find_potential_parent_threads()

    return {
        "current_thread": current_info,
        "potential_parents": potential_parents,
    }
