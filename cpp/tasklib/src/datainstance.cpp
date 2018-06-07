#include "datainstance.h"
#include "utils.h"
#include "log.h"

#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>


tasklib::DataInstance::~DataInstance()
{

}

cbor_item_t *tasklib::DataInstance::make_output_spec(cbor_item_t *output_item) const
{
    cbor_item_t *root = cbor_new_definite_map(3);

    cbor_item_t *info = cbor_new_definite_map(0);
    cbor_map_add(root, (struct cbor_pair) {
       .key = cbor_move(cbor_build_string("info")),
       .value = cbor_move(info)
    });

    cbor_map_add(root, (struct cbor_pair) {
       .key = cbor_move(cbor_build_string("location")),
       .value = cbor_move(make_location())
    });

    return root;
}

tasklib::DataInstancePtr tasklib::DataInstance::from_input_spec(cbor_item_t *item)
{
   cbor_item_t *location = cb_map_lookup(item, "location");

   cbor_item_t *location_type = cbor_array_get(location, 0);
   std::string location_type_str = cb_to_string(location_type);
   cbor_decref(&location_type);

   cbor_item_t *location_data = cbor_array_get(location, 1);

   DataInstancePtr result;
   if (location_type_str == "memory") {
      unsigned char *ptr = cbor_bytestring_handle(location_data);
      size_t len = cbor_bytestring_length(location_data);
      std::vector<unsigned char> data(ptr, ptr + len);
      result = std::make_unique<MemDataInstance>(std::move(data));
   } else if (location_type_str == "path") {
      std::string path = cb_to_string(location_data);
      result = std::make_unique<FileDataInstance>(path);
   } else {
      logger->critical("Unknown location type: '{}'", location_type_str);
      exit(1);
   }
   cbor_decref(&location_data);
   return result;
}

std::string tasklib::DataInstance::read_as_string()
{
   auto p = get_ptr();
   std::string s(p, p + get_size());
   return s;
}

tasklib::MemDataInstance::MemDataInstance(std::vector<unsigned char> &&data)
   : data(std::move(data))
{

}

tasklib::MemDataInstance::~MemDataInstance()
{

}

cbor_item_t *tasklib::MemDataInstance::make_location() const
{
    cbor_item_t *root = cbor_new_definite_array(2);
    cbor_array_push(root, cbor_move(cbor_build_string("memory")));
    cbor_array_push(root, cbor_move(cbor_build_bytestring(&data[0], data.size())));
    return root;
}

tasklib::FileDataInstance::FileDataInstance(const std::string &path) : path(path), data(nullptr), size(INVALID_SIZE)
{

}

tasklib::FileDataInstance::~FileDataInstance()
{
    if (data) {
        assert(size != INVALID_SIZE);
        munmap(data, size);
    }
}

size_t tasklib::FileDataInstance::get_size() const
{
    std::lock_guard<std::mutex> lock(mutex);
    if (INVALID_SIZE == size) {
        size = file_size(path.c_str());
    }
    return size;
}

const unsigned char *tasklib::FileDataInstance::get_ptr() const
{
    std::lock_guard<std::mutex> lock(mutex);
    if (!data) {
        if (INVALID_SIZE == size) {
            size = file_size(path.c_str());
        }
        int flags = PROT_READ;
        int fd = open();
        data = (unsigned char*) mmap(0, size, flags, MAP_SHARED, fd, 0);
        close(fd);
        if (data == MAP_FAILED) {
            logger->critical("Cannot mmap filename={}", path);
            log_errno_and_exit("mmap");
        }
    }
    return data;
}

int tasklib::FileDataInstance::open() const
{
    int fd = ::open(path.c_str(), O_RDONLY,  S_IRUSR | S_IWUSR);
    if (fd < 0) {
        logger->critical("Cannot open data {}", path);
        log_errno_and_exit("open");
    }
    return fd;
}

cbor_item_t *tasklib::FileDataInstance::make_location() const
{
    cbor_item_t *root = cbor_new_definite_array(2);
    cbor_array_push(root, cbor_move(cbor_build_string("path")));
    cbor_array_push(root, cbor_move(cbor_build_string(path.c_str())));
    return root;
}
