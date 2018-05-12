#include "subworker.h"

#include <stdio.h>

int main()
{
   rainsw::Subworker subworker("cpptester");

   subworker.add_task("hello", [](rainsw::Context &ctx, auto &inputs, auto &outputs) {
       if (!ctx.check_n_args(1)) {
           return;
       }
       auto& input1 = inputs[0];
       std::string str = "Hello " + input1->read_as_string() + "!";
       outputs.push_back(std::make_unique<rainsw::MemDataInstance>(str));
   });

   subworker.add_task("fail", [](rainsw::Context &ctx, auto &inputs, auto &outputs) {
       if (!ctx.check_n_args(1)) {
           return;
       }
       auto& input1 = inputs[0];
       ctx.set_error(input1->read_as_string());
   });

   subworker.start();
}
