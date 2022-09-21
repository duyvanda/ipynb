import subprocess
import sys


def run_command(command, env=None):
    if env is None:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            shell=True,
        )
    else:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            shell=True,
            env=env,
        )
    output, stderr = [
        stream.decode(sys.getdefaultencoding(), "ignore")
        for stream in process.communicate()
    ]
    return_code = process.returncode

    # case java
    if ("java " in command) & (("-cp" in command) | ("-jar" in command)):
        if "Exception" in stderr:
            raise RuntimeError(
                "Cannot execute {}.\n|Error code is: {}.\n|Output: {}.\n|Stderr: {}".format(
                    command, return_code, output, stderr
                )
            )
        else:
            return return_code, output, stderr

    if process.returncode != 0:
        raise RuntimeError(
            "Cannot execute {}.\n|Error code is: {}.\n|Output: {}.\n|Stderr: {}".format(
                command, return_code, output, stderr
            )
        )

    return return_code, output, stderr
