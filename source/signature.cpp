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

#include <boost/regex.hpp>
#include <boost/program_options.hpp>
#include <boost/crc.hpp>

#include <thool/thread_pool.hpp>

namespace bpo = boost::program_options;

namespace
{

constexpr size_t BLOCK_SIZE_KILOBYTE = 1024;
constexpr size_t BLOCK_SIZE_MEGABYTE = 1024 * BLOCK_SIZE_KILOBYTE;
constexpr size_t BLOCK_SIZE_GIGABYTE = 1024 * BLOCK_SIZE_MEGABYTE;
constexpr size_t BLOCK_CRC_MAP_PROCESSING_SIZE = 100;

}

/**
 *
 */
class block_size
{
   uint64_t number_;
   uint64_t suffix_;

public:
   block_size() : number_(0), suffix_(1)
   { }
   block_size(uint64_t number) : number_(number), suffix_(1)
   { }

   // Sets numeric and suffix parts of the block size and checks them to be correct.
   bool set(uint64_t number, char suffix)
   {
      uint64_t tmp = 1;

      if (suffix != 0)
      {
         // If suffix was provided check it to be in range (K, M, G).
         switch (suffix)
         {
            case 'K': tmp = BLOCK_SIZE_KILOBYTE; break;
            case 'M': tmp = BLOCK_SIZE_MEGABYTE; break;
            case 'G': tmp = BLOCK_SIZE_GIGABYTE; break;

            default: return false;
         }
      }

      // Check values of a numeric part of the block size and it's suffix
      // to be in range of uint64_t type storage size.
      if (std::numeric_limits<uint64_t>::max() / tmp < number)
         return false;
      if (std::numeric_limits<uint64_t>::max() / number < tmp)
         return false;

      uint64_t bs = number * tmp;

      if (bs < BLOCK_SIZE_KILOBYTE)
         return false;

      number_ = number;
      suffix_ = tmp;

      return true;
   }

   uint64_t get() const
   {
      return number_ * suffix_;
   }
};

/**
 *
 */
void validate(boost::any & value, const std::vector<std::string> & string_values, block_size * target_type, int)
{
   static const boost::regex r("(^\\d+)([K|M|G]?$)");

   boost::program_options::validators::check_first_occurrence(value);
   boost::smatch match;

   if (regex_match(boost::program_options::validators::get_single_string(string_values), match, r))
   {
      uint64_t number;
      std::string suffix;

      try
      {
         // Parse numeric part of the block size option.
         number = boost::lexical_cast<uint64_t>(match[1]);
         // Parse suffix to char if it was provided.
         suffix = boost::lexical_cast<std::string>(match[2]);
      }
      catch (boost::bad_lexical_cast & e)
      {
         throw boost::program_options::validation_error
         (
               boost::program_options::validation_error::invalid_option_value
         );
      }

      block_size bs;

      if (bs.set(number, (suffix.size() > 0) ? suffix[0] : 0))
      {
         value = boost::any(bs);
         return;
      }
   }
   throw boost::program_options::validation_error
   (
         boost::program_options::validation_error::invalid_option_value
   );
}

/**
 *
 */
int main(int argc, char ** argv)
{
   // Set default block size.
   block_size block_size_value { BLOCK_SIZE_MEGABYTE };

   std::string input_file_name, output_file_name;
   bpo::options_description desc;
   bpo::variables_map vm;

   // Form a map of application options that provide user interface using boost::program_options.
   desc.add_options()
         ("help,h", "print help")
         ("input,i",  bpo::value<std::string>(&input_file_name)->required(),   "input file")
         ("output,o", bpo::value<std::string>(&output_file_name)->required(),  "output file to store input file's signature")
         ("block,b",  bpo::value<block_size> (&block_size_value),              "size of a processing block in bytes (1K, 1M, 1G)");

   try
   {
      // Process application arguments.
      bpo::store(bpo::parse_command_line(argc, argv, desc), vm);
      bpo::notify(vm);
   }
   catch (std::exception & e)
   {
      std::cerr << e.what() << std::endl;
      return EXIT_FAILURE;
   }

   // Check application arguments for special cases and limitations.
   if (vm.count("help") || (argc == 1))
   {
      std::cout << desc << std::endl;
      return EXIT_SUCCESS;
   }
   if (input_file_name == output_file_name)
   {
      std::cerr << "input and output files are same" << std::endl;
      return EXIT_FAILURE;
   }

   // Print information about processing details.
   std::cout << "input  file = " << input_file_name        << std::endl;
   std::cout << "output file = " << output_file_name       << std::endl;
   std::cout << "block  size = " << block_size_value.get() << std::endl;


   std::ifstream input_file { input_file_name, std::ios::binary };

   if (!input_file.is_open())
   {
      std::cerr << "can't open input file" << std::endl;
      return EXIT_FAILURE;
   }

   std::ofstream output_file_stream { output_file_name, std::ios::binary | std::ios::trunc };

   if (!output_file_stream.is_open())
   {
      std::cerr << "can't open output file" << std::endl;
      return EXIT_FAILURE;
   }

   uint64_t block_counter = 0;
   uint64_t last_processed_block_id = 0;

   // Map of crc of blocks ordered by block number.
   std::map <uint64_t, uint32_t> block_crc_map;
   // Mutex for map that will be accessed through several threads.
   std::mutex block_crc_map_mutex;

   // Lambda for output file operations such as saving a crc of a block into a file according to block id.
   auto crc_saver = [&output_file_stream, &block_crc_map, &last_processed_block_id]()
   {
      while (true)
      {
         auto block = block_crc_map.find(last_processed_block_id);

         if (block_crc_map.end() != block)
         {
            output_file_stream.write(reinterpret_cast<char *>(&block->second), sizeof(block->second));
            last_processed_block_id++;
         }
         else break;
      }
   };

   // Get an instance of the thread pool.
   auto & tp = thool::thread_pool::instance();

   while (!input_file.eof())
   {
      // Create temporary buffer to get block's data from input file.
      std::shared_ptr<std::vector<char>> buffer_ptr;
      std::streamsize readed_size;

      try
      {
         buffer_ptr = std::make_shared<std::vector<char>>(block_size_value.get(), 0);
         input_file.read(buffer_ptr->data(), buffer_ptr->size());
         readed_size = input_file.gcount();

         // ToDo more intellectual exception handling
      }
      catch (std::exception & e)
      {
         std::cerr << e.what() << std::endl;
         break;
      }

      //
      auto task = [buffer_ptr, readed_size, &block_crc_map, &block_crc_map_mutex, block_counter]()
      {
         boost::crc_32_type crc_hash;
         crc_hash.process_bytes(buffer_ptr->data(), readed_size);
         boost::crc_32_type::value_type crc_value = crc_hash.checksum();
         std::lock_guard<std::mutex> lock(block_crc_map_mutex);
         if (!block_crc_map.insert({ block_counter, crc_value }).second)
         {
            std::cerr << "block " << block_counter << " insert error" << std::endl; // FixMe
         }
      };
      tp.add_task
      (
            std::make_shared<thool::task>(std::move(task), 0)
      );
      block_counter++;

      //
      std::lock_guard<std::mutex> lock(block_crc_map_mutex);
      //
      if (block_crc_map.size() >= BLOCK_CRC_MAP_PROCESSING_SIZE)
         crc_saver();
   }

   //
   while (last_processed_block_id != block_counter)
   {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      std::lock_guard<std::mutex> lock(block_crc_map_mutex);
      crc_saver();
   }
   tp.stop();

   std::cout << "done" << std::endl;

   input_file.close();
   output_file_stream.close();

   return EXIT_SUCCESS;
}
