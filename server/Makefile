
# All Target
all:
	g++ -std=c++11 -O3 -g -c -o  ParameterServer.o  ParameterServer.cc
	g++ -std=c++11 -O3 -g -c -o ../byteps/common/dma_common.o ../byteps/common/dma_common.cc
	g++ -std=c++11 -O3 -g -c -o ../byteps/common/HashTable.o ../byteps/common/HashTable.cc
	g++ -std=c++11 -O3 -g -o app  ParameterServer.o ../byteps/common/HashTable.o ../byteps/common/dma_common.o -lpthread -libverbs


# Clean Target
clean:
	rm *.o
	rm app
