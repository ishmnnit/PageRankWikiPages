#include<map>
#include<sstream>
#include<iostream>
#include<fstream>
#include<ostream>
#include<istream>
#include<mpi.h>
#include<string>
#include<cstring>
#include<cstdlib>
#include<map>
#include<cmath>
using namespace std;

int size,KEYSIZE;

int main(int argc,char **argv)
{

  int a,b;
  int actsize;
  char c;
  int cnt=0,nprocs,rank;

  MPI_Init(&argc,&argv);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);
  map<int,int> mymap;

  /* Calculate Size of Key Value Pair
     Mymap is used to map the key with contguous value */

  if(rank==0)
  {
     size=0;
     KEYSIZE=0;
     ifstream myfile;
     myfile.open("100000_key-value_pairs.csv");
     string line;
     myfile>>line;
     while(myfile>>a>>c>>b)
     {
           size++;
           if(mymap.find(a)==mymap.end())
           {   mymap[a]=KEYSIZE;
               KEYSIZE++;
           }
     }
     actsize=size;
     if(size%nprocs!=0)
      {
          int padding=nprocs-(size%nprocs);
          size=size+padding;
      }    
  }

  MPI_Bcast(&KEYSIZE,1,MPI_INT,0,MPI_COMM_WORLD);
  MPI_Bcast(&size,1,MPI_INT,0,MPI_COMM_WORLD);

  int matrix[size][2];
  if(rank==0)
  {
    ifstream myfile;  
    myfile.open("100000_key-value_pairs.csv");
    string line;
    myfile>>line;
    while(myfile>>a>>c>>b)
    { 
       matrix[cnt][0]=mymap[a];
       matrix[cnt][1]=b;
       cnt++;
    }

    if(actsize%nprocs!=0)
     {
         int padding=nprocs-(actsize%nprocs);
         map<int,int>::iterator it=mymap.begin();
         for(int i=0;i<padding;i++)
            {
                matrix[cnt][0]=it->second;
                matrix[cnt][1]=0;
                cnt++;
            }
     }   

  }

 int recmatrix[size][2];
 /*  Do MPI_Scatter  to the matrix , so that each processor will receive size/Numberofprocessor element */
 MPI_Scatter(matrix,(size/nprocs)*2,MPI_INT,recmatrix,(size/nprocs)*2, MPI_INT,0,MPI_COMM_WORLD);
 int finmatrix[nprocs][KEYSIZE];
 int recmatrix2[KEYSIZE];
 for(int i=0;i<KEYSIZE;i++)
     recmatrix2[i]=0;

 for(int i=0;i<(size/nprocs);i++)
     recmatrix2[recmatrix[i][0]]=recmatrix2[recmatrix[i][0]]+recmatrix[i][1];


 /*Do MPI_Allgather so that each processor have  result after phase in 2 D matrix */
 MPI_Allgather(recmatrix2,KEYSIZE, MPI_INT,finmatrix,KEYSIZE, MPI_INT,MPI_COMM_WORLD);
 
  int chunk=ceil((float)KEYSIZE/(float)nprocs);
  int result[chunk];
  int start=chunk*rank;
  int end=chunk*(rank+1);
  if(end > KEYSIZE)
      end=KEYSIZE;

  for(int i=start,j=0;i<end;i++,j++)
     {
             result[j]=0;
             for(int k=0;k<nprocs;k++)
             {
                    result[j]=result[j]+finmatrix[k][i];
             }

     }


 int recvcount[nprocs],displ[nprocs]; 
 for(int i=0;i<nprocs;i++)
    {
        int start=chunk*i;
        int end=chunk*(i+1);
        if(end > KEYSIZE)
            end=KEYSIZE;
        recvcount[i]=end-start;
        if(i==0)
            displ[i]=0;
        else
            displ[i]=displ[i-1]+recvcount[i-1];
    }

 int finresult[KEYSIZE];

 /* Collect all the result and write the Output in “Output_Task2”*/
 MPI_Gatherv(&result,end-start,MPI_INT,&finresult,recvcount, displ, MPI_INT,0,MPI_COMM_WORLD); 
 if(rank==0)
 {
     ofstream outfile; 
     stringstream ss;
     ss << "Output_Task2.txt";
     string str = ss.str(); 
     outfile.open (str.c_str());
     map<int,int>::iterator it;
     it=mymap.begin();
     for(;it!=mymap.end();it++)
        outfile<<it->first<<"    "<<finresult[it->second]<<"\n";
     outfile.close();
  }
 MPI_Finalize ();
 return 0;
}
