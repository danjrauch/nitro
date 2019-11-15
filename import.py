from mpi4py import MPI

rank = MPI.COMM_WORLD.Get_rank()
print('Hello from rank {0}'.format(rank))