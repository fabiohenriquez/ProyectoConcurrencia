"""


Proyecto: Contador de Palabras con Hadoop - MapReduce
Clase: Concurrencia de Sistemas Distribuidos 

Integrantes:
Fabio Alejandro Henriquez    11711109
Leonard Ariel Banegas        11711179 



"""

archivo=open("Data.txt","r")
archivo2=open("Dataset_Limpio.txt","w")

listaArchivo=[]
listaGuardar=[]


Diccionario=["about", "above", "across", "after", 
"against", "along", "among", "around", "at",
  "because of", "before", "behind", "below", 
  "beneath", "beside", "besides", "between",
  "beyond", "but", "by", "concerning", "despite", 
  "down", "during", "except", "excepting",
  "for", "from", "in", "in front of", "inside", 
  "in spite of", "instead of", "into",
  "like", "near", "of", "off", "onto", "out", 
  "outside", "over", "past", "regarding",
  "since", "through", "throughout", "to", "toward", 
  "under", "underneath", "until", "up",
  "upon", "up to", "with", "within", "without", 
  "with regard to", "with respect to","and","the",
  "my","how","did","you","nd","th","i","st","de","s",
"In","How","I","by","the","a","And","is","it","do","your","can","be","we",
"are","on","should","could","and","not","when","get","what","or","does","why",
"as","way","that","this","any","have","where","doesnt","cant","no","if",
"use","an","add","all","vs","ie","run","id","its","none","me","os","then",
"was","put","one","there","make","work","two","wont","has","d","r","t","n","in"
,"only","another","same","than","different","other","has","many","whats","some","cannot","more"]




for i in range(0,498196):
    listaArchivo.append(archivo.readline())



for i in range(0,len(listaArchivo)):
    linea=listaArchivo[i]
    

    #Caracteres no validos
    temporal=""

    for j in linea:
        if(j.isalpha() or j=='\n' or j==' '):
            temporal=temporal+j
        elif (j=='-' or j=='/'):
            temporal=temporal+' '


    temporal=temporal.lower()
    for j in Diccionario:
        prep1=j+' '
        prep2=' '+j+' '

    
        if(prep1 in temporal):
            if(prep1[0]==temporal[0] and prep1[1]==temporal[1]):
                temporal=temporal.replace(prep1,"")

    
        if(prep2 in temporal):
            temporal=temporal.replace(prep2," ")
    
    listaGuardar.append(temporal)
    print(temporal)
for i in listaGuardar:
    archivo2.write(i)



archivo.close()

