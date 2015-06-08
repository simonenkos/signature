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
 * Supporting class to handle a size of a block which consists of two
 * parts: a numeric part and a suffix, represented by K (kilobytes),
 * M (megabytes), and G (gigabytes) symbols.
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

   // Sets a numeric part and a suffix of the block_size and checks them to be correct.
   bool set(uint64_t number, char suffix)
   {
      uint64_t tmp = 1;

      if (suffix != 0)
      {
         // If suffix was provided, check if it's in range (K, M, G).
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
 * Overload function for validation of block_size class objects needed for boost::program_options.
 */
void validate(boost::any & value, const std::vector<std::string> & string_values, block_size * target_type, int)
{
   static const boost::regex r("(^\\d+)([K|M|G]?$)");

   boost::program_options::validators::check_first_occurrence(value);
   boost::smatch match;

   // Match parts of block_size according to regex.
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

int main(int argc, char ** argv)
{
   // Set default block size.
   block_size block_size_value { BLOCK_SIZE_MEGABYTE };

   std::string input_file_name, output_file_name;
   bpo::options_description help_desc, main_desc, desc;
   bpo::variables_map vm;

   // Make a map of application options that provide user interface using boost::program_options.
   help_desc.add_options()
         ("help,h", "print help");
   main_desc.add_options()
         ("input,i",  bpo::value<std::string>(&input_file_name)->required(),   "input file")
         ("output,o", bpo::value<std::string>(&output_file_name)->required(),  "output file to store input file's signature")
         ("block,b",  bpo::value<block_size> (&block_size_value),              "size of a processing block in bytes (1K, 1M, 1G)");
   desc.add(help_desc).add(main_desc);

   try
   {
      // Process application arguments.
      bpo::store(bpo::parse_command_line(argc, argv, desc), vm);

      if (vm.count("help") || (argc == 1))
      {
         std::cout << desc << std::endl;
         return EXIT_SUCCESS;
      }

      bpo::notify(vm);
   }
   catch (std::exception & e)
   {
      std::cerr << e.what() << std::endl;
      std::cout << desc << std::endl;
      return EXIT_FAILURE;
   }

   // Check application arguments for special cases and limitations.
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
         // Search for a checksum for a current block.
         auto block = block_crc_map.find(last_processed_block_id);

         if (block_crc_map.end() != block)
         {
            // If the checksum for the block has been calculated, save it
            // to the output stream and move to a next block.
            output_file_stream.write(reinterpret_cast<char *>(&block->second), sizeof(block->second));
            last_processed_block_id++;
         }
         else break;
      }
   };

   // Get an instance of the thread pool.
   auto & tp = thool::thread_pool::instance();

   // Reading a data from the input stream till the end.
   while (!input_file.eof())
   {
      // Create a temporary buffer to get block's data from input file.
      std::shared_ptr<std::vector<char>> buffer_ptr;
      std::streamsize readed_size;

      try
      {
         // Allocate vector of size block_size and put it to a shared pointer,
         // cause it should be available for a task out of scope of the cycle.
         buffer_ptr = std::make_shared<std::vector<char>>(block_size_value.get(), 0);
         // Read data from input stream to buffer, which size is equal to block_size.
         input_file.read(buffer_ptr->data(), buffer_ptr->size());
         // Get real amount of data that was read.
         readed_size = input_file.gcount();
      }
      catch (const std::bad_alloc & err)
      {
         // Not enough memory to create a new buffer. Wait till active tasks will be finished.
         std::this_thread::sleep_for(std::chrono::milliseconds(10));
         // And repeat current cycle.
         continue;
      }
      catch (const std::exception & err)
      {
         std::cerr << "unexpected exception: " << err.what() << std::endl;
         tp.stop();
         return EXIT_FAILURE;
      }

      //
      auto task = [buffer_ptr, readed_size, &block_crc_map, &block_crc_map_mutex, block_counter]()
      {
         boost::crc_32_type crc_hash;
         // Calculate CRC32 hash for a given data in the buffer.
         crc_hash.process_bytes(buffer_ptr->data(), readed_size);
         // Get resulting checksum.
         boost::crc_32_type::value_type crc_value = crc_hash.checksum();
         // Free up memory hold by buffer.
         buffer_ptr->clear();
         buffer_ptr->shrink_to_fit();

         while (true)
         {
            try
            {
               std::lock_guard<std::mutex> lock(block_crc_map_mutex);
               // Inserting a checksum of the block into the map to keep order of blocks.
               block_crc_map.insert({ block_counter, crc_value });
               // Break out of the cycle.
               break;
            }
            catch (const std::bad_alloc & err)
            {
               // Put task to sleep, to wait for free memory.
               std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
         }
      };
      tp.add_task
      (
            std::make_shared<thool::task>(std::move(task), 0)
      );
      block_counter++;

      std::lock_guard<std::mutex> lock(block_crc_map_mutex);
      // Wait while map will have a number of checksums to be processed.
      if (block_crc_map.size() >= BLOCK_CRC_MAP_PROCESSING_SIZE) crc_saver();
   }

   while (last_processed_block_id != block_counter)
   {
      // Processing remaining checksums by calling crc_saver every 10 ms
      // to be sure if some tasks are finished.
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
