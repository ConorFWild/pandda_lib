import subprocess

submit_command = "qsub -pe smp 6 -l m_mem_free=30G -q medium.q -o {out_path} -e {err_path}.q {job_script_path}"


class QSubScheduler:
    def __init__(self,
                 tmp_dir,
                 ):
        self.tmp_dir = tmp_dir

    def submit(self, job):
        # Write job to file
        job_script_path = self.tmp_dir / job.name
        with open(job_script_path, 'w') as f:
            f.write(job.script)

        out_path = self.tmp_dir / f"{job.name}.out"
        err_path = self.tmp_dir / f"{job.name}.err"

        # Get submit command
        _submit_command = submit_command.format(
            job_script_path=job_script_path,
            out_path=out_path,
            err_path=err_path
        )

        # Submit
        p = subprocess.Popen(
            _submit_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = p.communicate()
        print(stdout)
        print(stderr)
