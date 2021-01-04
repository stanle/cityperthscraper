import os
import subprocess

from tabula.errors import JavaNotFoundError
from tabula.io import JAVA_NOT_FOUND_ERROR, _jar_path, build_options, logger


def _run(java_options, options, path=None, encoding="utf-8", java_path: str = None):
    """Call tabula-java with the given lists of Java options and tabula-py
    options, as well as an optional path to pass to tabula-java as a regular
    argument and an optional encoding to use for any required output sent to
    stderr.
    tabula-py options are translated into tabula-java options, see
    :func:`build_options` for more information.
    """
    # Workaround to enforce the silent option. See:
    # https://github.com/tabulapdf/tabula-java/issues/231#issuecomment-397281157
    if options.get("silent"):
        java_options.extend(
            (
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=off",
                "-Dorg.apache.commons.logging.Log"
                "=org.apache.commons.logging.impl.NoOpLog",
            )
        )

    built_options = build_options(**options)
    args = [java_path or "java"] + java_options + ["-jar", _jar_path()] + built_options
    if path:
        args.append(path)

    try:
        result = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            check=True,
        )
        if result.stderr:
            logger.warning("Got stderr: {}".format(result.stderr.decode(encoding)))
        return result.stdout
    except FileNotFoundError:
        raise JavaNotFoundError(JAVA_NOT_FOUND_ERROR)
    except subprocess.CalledProcessError as e:
        logger.error("Error from tabula-java:\n{}\n".format(e.stderr.decode(encoding)))
        raise