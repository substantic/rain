#include "executor.h"

#include <stdio.h>

int main()
{
   tasklib::Executor executor("cpptester");

   executor.add_task("hello", [](tasklib::Context &ctx, auto &inputs, auto &outputs) {
       if (!ctx.check_n_args(1)) {
           return;
       }
       auto& input1 = inputs[0];
       std::string str = "Hello " + input1->read_as_string() + "!";
       outputs.push_back(std::make_unique<tasklib::MemDataInstance>(str));
   });

   executor.add_task("fail", [](tasklib::Context &ctx, auto &inputs, auto &outputs) {
       if (!ctx.check_n_args(1)) {
           return;
       }
       auto& input1 = inputs[0];
       ctx.set_error(input1->read_as_string());
   });

   executor.add_task("panic", [](tasklib::Context &ctx, auto &inputs, auto &outputs) {
       if (!ctx.check_n_args(0)) {
           return;
       }
       fprintf(stderr, "The task panicked on purpose, by calling task 'panic'");
       abort();
   });


   executor.start();
}
