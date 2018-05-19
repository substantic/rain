#ifndef RAINSW_DATAOBJECT_H
#define RAINSW_DATAOBJECT_H

#include <memory>
#include <vector>
#include <mutex>
#include <cbor.h>

#include "ids.h"

namespace tasklib {

class DataInstance;
using DataInstancePtr = std::unique_ptr<DataInstance>;
using DataInstanceVec = std::vector<DataInstancePtr>;

class DataInstance
{

public:
   DataInstance() {}
   virtual ~DataInstance();

   DataInstance(const DataInstance& that) = delete;
   DataInstance& operator=(DataInstance const&) = delete;

   virtual size_t get_size() const = 0;
   virtual const unsigned char* get_ptr() const = 0;

   cbor_item_t* make_output_spec(cbor_item_t *output_item) const;
   static DataInstancePtr from_input_spec(cbor_item_t *item);

   std::string read_as_string();

protected:

   virtual cbor_item_t* make_location() const = 0;

    DataObjectId id;
};



class MemDataInstance : public DataInstance
{
public:
   MemDataInstance(std::vector<unsigned char> &&data);
   MemDataInstance(const std::string &str) : MemDataInstance(std::vector<unsigned char>(str.begin(), str.end())) {}

   ~MemDataInstance();

   size_t get_size() const override {
       return data.size();
   }

   const unsigned char* get_ptr() const override {
       return &data[0];
   }


protected:
   cbor_item_t* make_location() const override;
   std::vector<unsigned char> data;
};


class FileDataInstance : public DataInstance
{
public:

    FileDataInstance(const std::string &path);
    ~FileDataInstance();
    size_t get_size() const override;
    const unsigned char* get_ptr() const override;


protected:
    static constexpr size_t INVALID_SIZE = std::numeric_limits<size_t>::max();
    int open() const;

    std::string path;

    mutable unsigned char *data;
    mutable size_t size;
    mutable std::mutex mutex;

    cbor_item_t* make_location() const override;
};

}

#endif // RAINSW_DATAOBJECT_H
