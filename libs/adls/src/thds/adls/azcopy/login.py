import subprocess

from thds.core import cache, log, scope

_AZCOPY_LOGIN_WORKLOAD_IDENTITY = "azcopy login --login-type workload".split()
_AZCOPY_LOGIN_LOCAL_STATUS = "azcopy login status".split()
# device login is an interactive process involving a web browser,
# which is not acceptable for large scale automation.
# So instead of logging in, we check to see if you _are_ logged in,
# and if you are, we try using azcopy in the future.
logger = log.getLogger(__name__)


@cache.locking  # only run this once per process.
@scope.bound
def good_azcopy_login() -> bool:
    scope.enter(log.logger_context(dl=None))
    try:
        subprocess.run(_AZCOPY_LOGIN_WORKLOAD_IDENTITY, check=True, capture_output=True)
        logger.info("Azcopy login with workload identity, so we can use it for large file transfers")
        return True

    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    try:
        subprocess.run(_AZCOPY_LOGIN_LOCAL_STATUS, check=True)
        logger.info("Azcopy login with local token, so we can use it for large file transfers")
        return True

    except FileNotFoundError:
        logger.info(
            "azcopy is not installed or not on your PATH, so we cannot speed up large file transfers"
        )
    except subprocess.CalledProcessError as cpe:
        logger.warning(
            "You are not logged in with azcopy, so we cannot speed up large file transfers."
            f" Run `azcopy login` to fix this. Return code was {cpe.returncode}"
        )
    return False
