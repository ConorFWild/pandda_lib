import htcondor

class HTCondorScheduler:
    def __init__(self,
                 tmp_dir,
                 ):
        self.tmp_dir = tmp_dir

    def submit(self, job, cores=24, mem_per_core=8):
        # Get the scheduler
        schedd = htcondor.Schedd()

        # Write job to file
        job_script_path = self.tmp_dir / f"{job.name}.sh"
        with open(job_script_path, 'w') as f:
            f.write(job.script)

        out_path = self.tmp_dir / f"{job.name}.out"
        err_path = self.tmp_dir / f"{job.name}.err"

        # Get submit command
        hostname_job = htcondor.Submit({
            "executable": job_script_path,  # the program to run on the execute node
            "output": self.tmp_dir / f"{job.name}.out",
            "error": self.tmp_dir / f"{job.name}.err",
            "log": self.tmp_dir / f"{job.name}.log",  # this file will contain a record of what happened to the job
            "request_cpus": f"{cores}",  # how many CPU cores we want
            "request_memory": f"{mem_per_core}GB",  # how much memory we want
            # "request_disk": "128MB",  # how much disk space we want
        })

        # Submit
        submit_result = schedd.submit(hostname_job)
        print(f"Job was submitted: scheduler assigned id {submit_result.cluster()} and "
              f"{submit_result.classad().printJson()}")
