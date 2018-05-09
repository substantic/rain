#include "subworker.h"
#include "log.h"
#include "cbor.h"
#include "utils.h"

rainsw::Subworker::Subworker(const std::__cxx11::string &type) : type(type)
{
    spdlog::set_pattern("%H:%M:%S [%l] %v");
    spdlog::set_level(spdlog::level::debug);
}

void rainsw::Subworker::start()
{
   init();

   for(;;) {
        auto msg = connection.receive();
        process_message(msg);
   };
}

void rainsw::Subworker::process_message(std::vector<char> &data)
{
   logger->debug("Message received");
   struct cbor_load_result result;
   cbor_item_t *root = cbor_load(reinterpret_cast<unsigned char*>(&data[0]), data.size(), &result);

   if (!root) {
      logger->critical("Failed to parse cbor message");
      exit(1);
   }

   size_t root_size = cbor_array_size(root);
   if (root_size != 2) {
      logger->critical("Invalid type of data received");
      exit(1);
   }

   cbor_item_t *msg_type = cbor_array_get(root, 0);
   assert(msg_type);
   cbor_item_t *msg_data = cbor_array_get(root, 1);
   assert(msg_data);
   std::string msg_type_str = cb_to_string(msg_type);
   cbor_decref(&msg_type);
   cbor_decref(&root);

   if (msg_type_str == "call") {
      process_message_call(msg_data);
   } else {
      logger->critical("Unknown message: {}", msg_type_str);
      exit(1);
   }

   cbor_decref(&msg_data);
}

void rainsw::Subworker::process_message_call(cbor_item_t *msg_data)
{
   std::string method = cb_map_lookup_string(msg_data, "method");
   logger->info("Running method '{}'", method);
}

void rainsw::Subworker::init()
{
   logger->info("Starting subworker");

   char *socket_path = std::getenv("RAIN_SUBWORKER_SOCKET");
   if (!socket_path) {
       logger->error("Env variable 'RAIN_SUBWORKER_SOCKET' not found");
       logger->error("It seems that subworker is not running in Rain environment");
       exit(1);
   }

   char *subworker_id = std::getenv("RAIN_SUBWORKER_ID");
   if (!subworker_id) {
      logger->error("Env variable 'RAIN_SUBWORKER_ID' not found");
      exit(1);
   }
   size_t sw_id;
   std::stoi (subworker_id,&sw_id);
   connection.connect(socket_path);

   logger->info("Sending registration message ...");

   cbor_item_t *data = cbor_new_definite_map(3);
   cbor_map_add(data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("protocol")),
      .value = cbor_move(cbor_build_string("cbor-1"))
   });
   cbor_map_add(data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("subworkerType")),
      .value = cbor_move(cbor_build_string(type.c_str()))
   });
   cbor_map_add(data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("subworkerId")),
      .value = cbor_move(cbor_build_uint32(sw_id))
   });
   send_message("register", data);
}

void rainsw::Subworker::send_message(const char *name, cbor_item_t *data)
{
   cbor_item_t *root = cbor_new_definite_array(2);
   cbor_check(root);

   cbor_item_t *message_type = cbor_build_string(name);
   cbor_check(message_type);

   cbor_array_push(root, cbor_move(message_type));
   cbor_array_push(root, cbor_move(data));

   unsigned char *buffer = NULL;
   size_t buffer_size = 0;
   size_t data_len = cbor_serialize_alloc(root, &buffer, &buffer_size);
   assert(buffer);

   connection.send(buffer, data_len);
   free(buffer);
   cbor_decref(&root);
}


