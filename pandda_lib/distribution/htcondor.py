from time import sleep


class ClusterHTCondor:

    def __init__(self,
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
        self.cluster.scale(jobs=1)

        self.client = Client(self.cluster)

    def submit(self, f):
        # Multiprocess
        process = self.client.submit(f)
        while f.status == 'pending':
            sleep(0.1)

        if f.status == 'error':
            raise Exception(f'Failed to recreate errors in dask distribution locally!')

        result = process.result()
