from time import sleep
import time


class ClusterHTCondor:

    def __init__(self,
                 jobs,
                 cores_per_worker=12,
                 distributed_mem_per_core=10,
                 ):
        import dask
        from dask.distributed import Client
        from dask_jobqueue import HTCondorCluster, PBSCluster, SGECluster, SLURMCluster

        dask.config.set({'distributed.worker.daemon': False})

        job_extra = [(f"GetEnv", "True"), ]
        self.cluster = HTCondorCluster(
            cores=cores_per_worker,
            memory=f"{distributed_mem_per_core * cores_per_worker}G",
            disk="10G",
            processes=1,
            job_extra=job_extra,
        )
        self.cluster.scale(jobs=jobs)

        self.client = Client(self.cluster)

    def __call__(self, funcs, callback=None):
        processes = [self.client.submit(func) for func in funcs]

        time_started = time.time()
        while any(process.status == 'pending' for process in processes):
            # print(f"Process status is: {process.status}")
            sleep(0.1)
            current_time = time.time()
            if (current_time - time_started) % 60 < 1:
                print("#####################################")
                print(f"Statuses are: {[process.status for process in processes]}")
                if callback:
                    print(callback())

                time.sleep(1)

    def submit(self, f, callback):
        # Multiprocess
        process = self.client.submit(f)
        time_started = time.time()

        while process.status == 'pending':
            # print(f"Process status is: {process.status}")
            sleep(0.1)

            current_time = time.time()
            if (current_time - time_started) % 60 < 1:
                print("#####################################")
                print(f"Statuses are: {[process.status]}")
                if callback:
                    print(callback())

                time.sleep(1)
        #
        # if process.status == 'error':
        #     self.client.recreate_error_locally([process,])
        #     raise Exception(f'Failed to recreate errors in dask distribution locally!')

        result = process.result()

        return result
