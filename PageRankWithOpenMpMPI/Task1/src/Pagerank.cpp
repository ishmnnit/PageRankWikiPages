#include<vector>
#include<iostream>
#include<fstream>
#include<algorithm>
#include<string>
#include<cstdlib>
#include<iomanip>
#include<omp.h>
#include<cstring>
#include<set>
#include<sstream>
#include<cmath>
using namespace std;
#define NUM_THREAD 4
#define ERROR 0.0000000001

struct pairs
{
    int node;
    double rank;
};

int GraphSizeCalculation(char *argv)
{
    ifstream myfile;
    myfile.open(argv);      
    set<int> myset;
    int aa,bb;
    while (myfile>>aa>>bb )
    {
        myset.insert(aa);
        myset.insert(bb);
    }
    myfile.close();
    return myset.size();
}

void WriteFile(vector<double> rank,int size)
{    
    ofstream outfile;
    stringstream ss;
    ss << "Output_Task1.txt";
    string str = ss.str();
    outfile.open (str.c_str());
    for(int i=0;i<size;i++)
        outfile<<i<<"   "<<rank[i]<<"\n";                                       
    outfile.close();
}    



int main(int argc, char* argv[])
{
    double start_time,run_time;
    start_time=omp_get_wtime();

    /* Calulate Number of Nodes in the Graph*/
    int size=GraphSizeCalculation(argv[1]);

    int numlink[size];
    memset(numlink,0,sizeof(numlink));
    
    ifstream myfile;
    myfile.open(argv[1]);
    string line;
    /* Create a Sparse Matrix using Graph Node */ 
    vector< vector<pairs> > vec(size);
    int a,b;
    while ( myfile>>a>>b )
     {
          pairs p1,p2;
          p1.node=a; 
          p1.rank=1.0;
          p2.node=b; 
          p2.rank=1.0;
          vec[b].push_back(p1);
          vec[a].push_back(p2);
          numlink[a]=numlink[a]+1;
          numlink[b]=numlink[b]+1;
     }


    omp_set_num_threads(NUM_THREAD);
    #pragma omp parallel for
    for(int i=0;i<size;i++)
      {
          for(int j=0;j<vec[i].size();j++)
          {
                vec[i][j].rank=(double)(vec[i][j].rank/double(numlink[vec[i][j].node]));
          }
      }


     vector<double> rank(size);
     vector<double> newrank(size);
     int cnt=0;
     /* Normalization of Initial Page Rank */  
     #pragma omp parallel for
     for(int i=0;i<size;i++)
            rank[i]=1.0/(double)size;

      int count=0;
      double damp=0.85;
      /* Calculation of Page Rank */
      while(1)
      {   
          #pragma omp parallel for
          for(int i=0;i<size;i++)
          {
               double sum=0.0;
               int sz=vec[i].size();
               #pragma omp parallel for reduction(+:sum) 
               for(int j=0;j<sz;j++)
               {
                       sum=sum+(vec[i][j].rank)*(rank[vec[i][j].node]);
               }
               newrank[i]=(1-damp)/size+damp*sum;
         }
         
         /* Verification of Convergence of each Node in Graph*/  
         bool isBreak=true;
         for(int i=0;i<size;i++)
         {
             if(fabs(newrank[i]-rank[i])> ERROR)
                 isBreak=false;

             rank[i]=newrank[i];
         }
         cnt++;

         if(isBreak)
             break;
         count++;
      }


      /* Write Final Rank Into a file */
      WriteFile(rank,size);

      run_time=omp_get_wtime()-start_time;
      cout<<"No of Iteration in convergence---"<<count<<endl;
      cout<<"Total runtime---"<<run_time<<endl;
}
