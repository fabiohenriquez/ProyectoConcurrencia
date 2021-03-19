
/*



Proyecto: Contador de Palabras con Hadoop - MapReduce
Clase: Concurrencia de Sistemas Distribuidos 

Integrantes:


Fabio Alejandro Henriquez    11711109
Leonard Ariel Banegas        11711179 




*/

#include <iostream>
#include <string.h>
#include <fstream>
#include <vector>
#include <sstream>
#include <functional>
#include <numeric>
#include <algorithm>
using namespace std;
void Ordenamiento(string,string);


template<class T>
struct DefaultFuncComparar
{
  bool operator()(T const& a, T const& b)
  {
    return a > b;
  }
};


//Funcion del sort
template<class T, class U, class FuncComparar = std::function<bool(T const&, T const&)>>
void OrdenarVectores(
  std::vector<T>& a,
  std::vector<U>& b,
  FuncComparar comparar = DefaultFuncComparar<T>())
{
  std::vector<T> copiaA{a};
  std::vector<U> copiaB{b};

  
  std::vector<std::size_t> vector_indices(a.size());
  std::iota(vector_indices.begin(), vector_indices.end(), 0);


  std::sort(vector_indices.begin(), vector_indices.end(),
      [&](std::size_t i, std::size_t j){ return comparar(a[i], a[j]); });

 
  std::transform(vector_indices.begin(), vector_indices.end(), a.begin(),
      [&](std::size_t i){ return copiaA[i]; });

  std::transform(vector_indices.begin(), vector_indices.end(), b.begin(),
      [&](std::size_t i){ return copiaB[i]; });
}




int main()
{
    Ordenamiento("palabras1.txt","Resultados1.txt");
    Ordenamiento("palabras2.txt","Resultados2.txt");
  
    return 0;
}


void Ordenamiento(string abrir,string guardar){
    string texto;
    ifstream archivo;
    ofstream nuevo;
    char r=(char)9;

    vector<string> listaArchivo;
    vector<int> listaPos;
    vector<string> listaPalabras;
    archivo.open(abrir,ios::in);
    nuevo.open(guardar,ios::out);



    //Parte de analizar el archivo
    if(archivo.fail()){
        cout<<"El archivo no se abrio"<<endl;
    }else{
        if(nuevo.fail()){
            cout<<"El archivo no se abrio"<<endl;
        }else{
            getline(archivo,texto);
            while (!archivo.eof()) {
                getline(archivo,texto);
                listaArchivo.push_back(texto);
            }
        }
    }

    for (size_t y = 0; y < listaArchivo.size(); y++)
    {
        string final;
        string finalt;
        stringstream concatenar;
        stringstream numeros;
        bool bandera=false;
        for (size_t  i= 0; i < listaArchivo.at(y).size(); i++)
        {
            if(listaArchivo.at(y)[i]!=r && bandera==false){
                concatenar << listaArchivo.at(y)[i];
            }else{
              bandera=true;
            }

            if(bandera && listaArchivo.at(y)[i]!=r){
                numeros << listaArchivo.at(y)[i];
            }  
        }
        final= concatenar.str();
        finalt = numeros.str();

        int number;
        istringstream iss (finalt);
        iss >> number;


        listaPalabras.push_back(final);
        listaPos.push_back(number);
       
        
    }

    OrdenarVectores(listaPos,listaPalabras);
    
    for (size_t i = 0; i < listaPos.size(); i++)
  {
      nuevo<<listaPalabras.at(i)<<" "<<listaPos.at(i)<<"\n";
  }  

}
