#include <sstream>

#include "executor.h"
#include "log.h"
#include "cbor.h"
#include "utils.h"
#include "ids.h"
#include "datainstance.h"

tasklib::Executor::Executor(const std::string &type_name) : type_name(type_name)
{
    spdlog::set_pattern("%H:%M:%S [%l] %v");
    spdlog::set_level(spdlog::level::debug);
}

void tasklib::Executor::start()
{
   init();

   for(;;) {
        auto msg = connection.receive();
        process_message(msg);
   };
}

void tasklib::Executor::process_message(std::vector<char> &data)
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

void tasklib::Executor::process_message_call(cbor_item_t *msg_data)
{
   //cbor_describe(msg_data, stdout);
   cbor_item_t *spec = cb_map_lookup(msg_data, "spec");
   std::string method = cb_map_lookup_string(spec, "task_type");

   cbor_item_t *id_item = cb_map_lookup(spec, "id");
   cbor_incref(id_item);

   TaskId task_id = TaskId::from(id_item);

   logger->info("Running method '{}' (id = {})", method, task_id.to_string());

   auto fn = registered_tasks.find(&method[type_name.size() + 1]); // remove prefix
   if (fn == registered_tasks.end()) {
        send_error(std::string("Method '") + method + "' not found in executor", id_item);
        return;
   }

   cbor_item_t *inputs_item = cb_map_lookup(msg_data, "inputs");
   std::vector<DataInstancePtr> task_inputs;
   size_t len = cbor_array_size(inputs_item);
   task_inputs.reserve(len);
   for (size_t i = 0; i < len; i++) {
       cbor_item_t *input_item = cbor_array_get(inputs_item, i);
       task_inputs.push_back(DataInstance::from_input_spec(input_item));
       cbor_decref(&input_item);
   }

   cbor_item_t *outputs_item = cb_map_lookup(msg_data, "outputs");
   size_t len_out = cbor_array_size(outputs_item);
   std::vector<DataInstancePtr> task_outputs;
   task_outputs.reserve(len_out);

   Context ctx(task_inputs.size());
   fn->second(ctx, task_inputs, task_outputs); // Call the task function

   if (ctx.has_error()) {
       auto &error = ctx.get_error_message();
       logger->info("Method finished with error: {}", error);
       send_error(error, id_item);
       return;
   }

   logger->info("Method finished");

   if (len_out != task_outputs.size()) {
       std::stringstream s;
       s << "Task produced " << task_outputs.size() << " outputs, but expected " << len_out;
       send_error(s.str(), id_item);
       return;
   }

   cbor_item_t *outs = cbor_new_definite_array(cbor_array_size(outputs_item));
   for (size_t i = 0; i < len_out; i++) {
       cbor_item_t *o = cbor_array_get(outputs_item, i);
       cbor_array_push(outs, cbor_move(task_outputs[i]->make_output_spec(o)));
       cbor_decref(&o);
   }

   cbor_item_t *result_data = cbor_new_definite_map(4);
   cbor_map_add(result_data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("task")),
      .value = cbor_move(id_item),
   });
   cbor_map_add(result_data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("success")),
      .value = cbor_move(cbor_build_bool(true))
   });
   cbor_map_add(result_data, (struct cbor_pair) {
        .key = cbor_move(cbor_build_string("outputs")),
        .value = cbor_move(outs)
   });

   //cbor_tag_item *outputs_item = cbor_new_definite_array()

   cbor_item_t *info = cbor_new_definite_map(0);
   cbor_map_add(result_data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("info")),
      .value = cbor_move(info)
   });
   send_message("result", result_data);
}

void tasklib::Executor::send_error(const std::string &error_msg, cbor_item_t *id_item)
{
    // TODO: This needs real JSON serialization
    std::string message = '"' + error_msg + '"';
    cbor_item_t *result_data = cbor_new_definite_map(3);
    cbor_map_add(result_data, (struct cbor_pair) {
       .key = cbor_move(cbor_build_string("task")),
       .value = cbor_move(id_item)
    });
    cbor_map_add(result_data, (struct cbor_pair) {
       .key = cbor_move(cbor_build_string("success")),
       .value = cbor_move(cbor_build_bool(false))
    });

    cbor_item_t *info = cbor_new_definite_map(1);
    cbor_map_add(info, (struct cbor_pair) {
       .key = cbor_move(cbor_build_string("error")),
       .value = cbor_move(cbor_build_string(message.c_str()))
    });

    cbor_map_add(result_data, (struct cbor_pair) {
       .key = cbor_move(cbor_build_string("info")),
       .value = cbor_move(info)
    });
    send_message("result", result_data);
}

void tasklib::Executor::add_task(const std::string &name, const tasklib::TaskFunction &fn)
{
    registered_tasks[name] = fn;
}

void tasklib::Executor::init()
{
   logger->info("Starting executor");

   char *socket_path = std::getenv("RAIN_EXECUTOR_SOCKET");
   if (!socket_path) {
       logger->error("Env variable 'RAIN_EXECUTOR_SOCKET' not found");
       logger->error("It seems that executor is not running in Rain environment");
       exit(1);
   }

   char *executor_id = std::getenv("RAIN_EXECUTOR_ID");
   if (!executor_id) {
      logger->error("Env variable 'RAIN_EXECUTOR_ID' not found");
      exit(1);
   }
   size_t sw_id;
   std::stoi (executor_id,&sw_id);
   connection.connect(socket_path);

   logger->info("Sending registration message ...");

   cbor_item_t *data = cbor_new_definite_map(3);
   cbor_map_add(data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("protocol")),
      .value = cbor_move(cbor_build_string("cbor-1"))
   });
   cbor_map_add(data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("executor_type")),
      .value = cbor_move(cbor_build_string(type_name.c_str()))
   });
   cbor_map_add(data, (struct cbor_pair) {
      .key = cbor_move(cbor_build_string("executor_id")),
      .value = cbor_move(cbor_build_uint32(sw_id))
   });
   send_message("register", data);
}

void tasklib::Executor::send_message(const char *name, cbor_item_t *data)
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


