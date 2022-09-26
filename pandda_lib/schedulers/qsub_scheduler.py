import subprocess

submit_command = "qsub -pe smp {cores} -l m_mem_free={mem_per_core}G -o {out_path} -e {err_path}.q {" \
                 "job_script_path}"


class QSubScheduler:
    def __init__(self,
                 tmp_dir,
                 ):
        self.tmp_dir = tmp_dir

    def submit(self, job, cores=24, mem_per_core=8):
        # Write job to file
        job_script_path = self.tmp_dir / f"{job.name}.sh"
        with open(job_script_path, 'w') as f:
            f.write(job.script)


        out_path = self.tmp_dir / f"{job.name}.out"
        err_path = self.tmp_dir / f"{job.name}.err"

        # Get submit command
        _submit_command = submit_command.format(
            cores=cores,
            mem_per_core=mem_per_core,
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
