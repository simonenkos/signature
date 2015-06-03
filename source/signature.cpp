/*
 * signature.cpp
 *
 *  Created on: Jun 1, 2015
 *      Author: simonenkos
 */

#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <future>
#include <queue>
#include <memory>

#include <boost/program_options.hpp>

#include <thool/thread_pool.hpp>

namespace bpo = boost::program_options;

namespace
{

constexpr int MINIMUM_BLOCK_SIZE = 32;
constexpr int DEFAULT_BLOCK_SIZE = 1024 * 1024;
constexpr int MAXIMUM_BLOCK_SIZE = 1024 * 1024 * 512;

}

int main(int argc, char ** argv)
{
   int block_size = DEFAULT_BLOCK_SIZE;
   std::string input_file_name, output_file_name;
   bpo::options_description desc;
   bpo::variables_map vm;

   // Form a map of application options providing user interface.
   desc.add_options()
         ("help,h", "print help")
         ("input,i",  bpo::value<std::string>(&input_file_name)->required(),   "input file")
         ("output,o", bpo::value<std::string>(&output_file_name)->required(),  "output file to store input file's signature")
         ("block,b",  bpo::value<int>(&block_size),                            "size of a processing block in bytes");

   try
   {
      bpo::store(bpo::parse_command_line(argc, argv, desc), vm);
      bpo::notify(vm);
   }
   catch (std::exception & e)
   {
      std::cerr << e.what() << std::endl;
      return EXIT_FAILURE;
   }

   // Check help option.
   if (vm.count("help") || (argc == 1))
   {
      std::cout << desc << std::endl;
      return EXIT_SUCCESS;
   }

   // Check processing block size to be between minimum and maximum values.
   if ((block_size < MINIMUM_BLOCK_SIZE) || (block_size > MAXIMUM_BLOCK_SIZE))
   {
      std::cerr << "invalid block size: " << block_size << std::endl;
      return EXIT_FAILURE;
   }

   // Print information about processing details.
   std::cout << "input  file = " << input_file_name  << std::endl;
   std::cout << "output file = " << output_file_name << std::endl;
   std::cout << "block  size = " << block_size       << std::endl;

   std::ifstream input_file(input_file_name, std::ios::binary);

   if (!input_file.is_open())
   {
      std::cerr << "can't open input file" << std::endl;
      return EXIT_FAILURE;
   }

   std::ofstream output_file_stream(output_file_name, std::ios::binary | std::ios::trunc);
   std::mutex    output_file_mutex; // Mutex to protect stream.

   if (!output_file_stream.is_open())
   {
      std::cerr << "can't open output file" << std::endl;
      return EXIT_FAILURE;
   }

   // Get a thread pool instance.
   auto & tp = thool::thread_pool::instance();

   while (!input_file.eof())
   {
      auto buffer_ptr = std::make_shared<std::vector<char>>(block_size, 0);

      input_file.read(buffer_ptr->data(), buffer_ptr->size());

      auto readed_size = input_file.gcount();
      auto task = [buffer_ptr, readed_size, &output_file_stream, &output_file_mutex]()
      {
         std::lock_guard<std::mutex> lock(output_file_mutex);

         output_file_stream.write(buffer_ptr->data(), readed_size);
         output_file_stream.flush();
      };
      tp.add_task
      (
            std::make_shared<thool::task>(std::move(task), 0)
      );
   }

   tp.stop();

   std::cout << "done" << std::endl;

   input_file.close();
   output_file_stream.close();

   return EXIT_SUCCESS;
}
