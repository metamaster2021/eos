#include <eosio/chain/backing_store/kv_context_chainbase.hpp>
#include <eosio/chain/backing_store/db_combined.hpp>
#include <eosio/chain/combined_database.hpp>
#include <eosio/chain/kv_chainbase_objects.hpp>
#include <eosio/chain/backing_store/db_context.hpp>
#include <eosio/chain/backing_store/db_key_value_format.hpp>

namespace eosio { namespace chain {
   combined_session::combined_session(chainbase::database& cb_database)
      {
      cb_session = std::make_unique<chainbase::database::session>(cb_database.start_undo_session(true));
   }
   combined_session::combined_session(combined_session&& src) noexcept
       : cb_session(std::move(src.cb_session)) {
   }

   void combined_session::push() {
      if (cb_session) {
         cb_session->push();
         cb_session = nullptr;
      }
   }

   void combined_session::squash() {
      if (cb_session) {
         cb_session->squash();
         cb_session = nullptr;
      }
   }

   void combined_session::undo() {
      if (cb_session) {
         cb_session->undo();
         cb_session = nullptr;
      }
   }

   template <typename Util, typename F>
   void walk_index(const Util& utils, const chainbase::database& db, F&& function) {
      utils.walk(db, std::forward<F>(function));
   }

   template <typename F>
   void walk_index(const index_utils<table_id_multi_index>& utils, const chainbase::database& db, F&& function) {}

   template <typename F>
   void walk_index(const index_utils<database_header_multi_index>& utils, const chainbase::database& db, F&& function) {}

   template <typename F>
   void walk_index(const index_utils<kv_db_config_index>& utils, const chainbase::database& db, F&& function) {}

   void add_kv_table_to_snapshot(const snapshot_writer_ptr& snapshot, const chainbase::database& db) {
      snapshot->write_section<kv_object>([&db](auto& section) {
         index_utils<kv_index> utils;
         utils.walk<by_kv_key>(db, [&db, &section](const auto& row) { section.add_row(row, db); });
      });
   }

   void read_kv_table_from_snapshot(const snapshot_reader_ptr& snapshot, chainbase::database& db, uint32_t version) {
      if (version < kv_object::minimum_snapshot_version)
         return;
      snapshot->read_section<kv_object>([&db](auto& section) {
         bool more = !section.empty();
         while (more) {
            index_utils<kv_index>::create(db, [&db, &section, &more](auto &row) { more = section.read_row(row, db); });
         }
      });
   }

   combined_database::combined_database(chainbase::database& chain_db,
                                        uint32_t snapshot_batch_threashold)
       : backing_store(backing_store_type::CHAINBASE), db(chain_db) {}

   void combined_database::check_backing_store_setting(bool clean_startup) {
      if (backing_store != db.get<kv_db_config_object>().backing_store) {   
         EOS_ASSERT(clean_startup, database_move_kv_disk_exception,
                   "Existing state indicates a different backing store is in use; use resync, replay, or restore from snapshot to switch backing store");
         db.modify(db.get<kv_db_config_object>(), [this](auto& cfg) { cfg.backing_store = backing_store; });
      }
   }

   void combined_database::destroy(const fc::path& p) {
      if( !fc::is_directory( p ) )
          return;

      fc::remove( p / "shared_memory.bin" );
      fc::remove( p / "shared_memory.meta" );
   }

   void combined_database::set_revision(uint64_t revision) {
      db.set_revision(revision);
   }

   int64_t combined_database::revision() {
      return db.revision();
   }

   void combined_database::undo() {
      db.undo();
   }

   void combined_database::commit(int64_t revision) {
      db.commit(revision);
   }

   void combined_database::flush() {
   }

   std::unique_ptr<kv_context> combined_database::create_kv_context(name receiver, kv_resource_manager resource_manager,
                                                                    const kv_database_config& limits) const {
      return create_kv_chainbase_context<kv_resource_manager>(db, receiver, resource_manager, limits);
   }

   std::unique_ptr<db_context> combined_database::create_db_context(apply_context& context, name receiver) {
      return backing_store::create_db_chainbase_context(context, receiver);
   }

   void combined_database::add_to_snapshot(
         const eosio::chain::snapshot_writer_ptr& snapshot, const eosio::chain::block_state& head,
         const eosio::chain::authorization_manager&                    authorization,
         const eosio::chain::resource_limits::resource_limits_manager& resource_limits) const {
      snapshot->write_section<chain_snapshot_header>(
            [this](auto& section) { section.add_row(chain_snapshot_header(), db); });

      snapshot->write_section<block_state>(
            [this, &head](auto& section) { section.template add_row<block_header_state>(head, db); });

      eosio::chain::controller_index_set::walk_indices([this, &snapshot](auto utils) {
         using value_t = typename decltype(utils)::index_t::value_type;

         snapshot->write_section<value_t>([utils, this](auto& section) {
            walk_index(utils, db, [this, &section](const auto& row) { section.add_row(row, db); });
         });
      });

      add_kv_table_to_snapshot(snapshot, db);
      add_contract_tables_to_snapshot(snapshot);

      authorization.add_to_snapshot(snapshot);
      resource_limits.add_to_snapshot(snapshot);
   }

   void combined_database::read_from_snapshot(const snapshot_reader_ptr& snapshot,
                                              uint32_t blog_start,
                                              uint32_t blog_end,
                                              eosio::chain::authorization_manager& authorization,
                                              eosio::chain::resource_limits::resource_limits_manager& resource_limits,
                                              eosio::chain::block_state_ptr& head, uint32_t& snapshot_head_block,
                                              const eosio::chain::chain_id_type& chain_id) {
      chain_snapshot_header header;
      snapshot->read_section<chain_snapshot_header>([this, &header](auto& section) {
         section.read_row(header, db);
         header.validate();
      });

      db.create<kv_db_config_object>([](auto&) {});
      check_backing_store_setting(true);

      { /// load and upgrade the block header state
         block_header_state head_header_state;
         using v2 = legacy::snapshot_block_header_state_v2;

         if (std::clamp(header.version, v2::minimum_version, v2::maximum_version) == header.version) {
            snapshot->read_section<block_state>([this, &head_header_state](auto& section) {
               legacy::snapshot_block_header_state_v2 legacy_header_state;
               section.read_row(legacy_header_state, db);
               head_header_state = block_header_state(std::move(legacy_header_state));
            });
         } else {
            snapshot->read_section<block_state>(
                  [this, &head_header_state](auto& section) { section.read_row(head_header_state, db); });
         }

         snapshot_head_block = head_header_state.block_num;
         EOS_ASSERT(blog_start <= (snapshot_head_block + 1) && snapshot_head_block <= blog_end, block_log_exception,
                    "Block log is provided with snapshot but does not contain the head block from the snapshot nor a "
                    "block right after it",
                    ("snapshot_head_block", snapshot_head_block)("block_log_first_num",
                                                                 blog_start)("block_log_last_num", blog_end));

         head = std::make_shared<block_state>();
         static_cast<block_header_state&>(*head) = head_header_state;
      }

      controller_index_set::walk_indices([this, &snapshot, &header](auto utils) {
         using value_t = typename decltype(utils)::index_t::value_type;

         // skip the table_id_object as its inlined with contract tables section
         if (std::is_same<value_t, table_id_object>::value) {
            return;
         }

         // skip the database_header as it is only relevant to in-memory database
         if (std::is_same<value_t, database_header_object>::value) {
            return;
         }

         // skip the kv_db_config as it only determines where the kv-database is stored
         if (std::is_same_v<value_t, kv_db_config_object>) {
            return;
         }

         // special case for in-place upgrade of global_property_object
         if (std::is_same<value_t, global_property_object>::value) {
            using v2 = legacy::snapshot_global_property_object_v2;
            using v3 = legacy::snapshot_global_property_object_v3;
            using v4 = legacy::snapshot_global_property_object_v4;

            if (std::clamp(header.version, v2::minimum_version, v2::maximum_version) == header.version) {
               std::optional<genesis_state> genesis = extract_legacy_genesis_state(*snapshot, header.version);
               EOS_ASSERT(genesis, snapshot_exception,
                          "Snapshot indicates chain_snapshot_header version 2, but does not contain a genesis_state. "
                          "It must be corrupted.");
               snapshot->read_section<global_property_object>(
                     [&db = this->db, gs_chain_id = genesis->compute_chain_id()](auto& section) {
                        v2 legacy_global_properties;
                        section.read_row(legacy_global_properties, db);

                        db.create<global_property_object>([&legacy_global_properties, &gs_chain_id](auto& gpo) {
                           gpo.initalize_from(legacy_global_properties, gs_chain_id, kv_database_config{},
                                              genesis_state::default_initial_wasm_configuration);
                        });
                     });
               return; // early out to avoid default processing
            }

            if (std::clamp(header.version, v3::minimum_version, v3::maximum_version) == header.version) {
               snapshot->read_section<global_property_object>([&db = this->db](auto& section) {
                  v3 legacy_global_properties;
                  section.read_row(legacy_global_properties, db);

                  db.create<global_property_object>([&legacy_global_properties](auto& gpo) {
                     gpo.initalize_from(legacy_global_properties, kv_database_config{},
                                        genesis_state::default_initial_wasm_configuration);
                  });
               });
               return; // early out to avoid default processing
            }

            if (std::clamp(header.version, v4::minimum_version, v4::maximum_version) == header.version) {
               snapshot->read_section<global_property_object>([&db = this->db](auto& section) {
                  v4 legacy_global_properties;
                  section.read_row(legacy_global_properties, db);

                  db.create<global_property_object>([&legacy_global_properties](auto& gpo) {
                     gpo.initalize_from(legacy_global_properties);
                  });
               });
               return; // early out to avoid default processing
            }

         }

         snapshot->read_section<value_t>([this](auto& section) {
            bool more = !section.empty();
            while (more) {
               decltype(utils)::create(db, [this, &section, &more](auto& row) { more = section.read_row(row, db); });
            }
         });
      });

      read_kv_table_from_snapshot(snapshot, db, header.version);
      read_contract_tables_from_snapshot(snapshot);

      authorization.read_from_snapshot(snapshot);
      resource_limits.read_from_snapshot(snapshot, header.version);

      set_revision(head->block_num);
      db.create<database_header_object>([](const auto& header) {
         // nothing to do
      });

      const auto& gpo = db.get<global_property_object>();
      EOS_ASSERT(gpo.chain_id == chain_id, chain_id_type_exception,
                 "chain ID in snapshot (${snapshot_chain_id}) does not match the chain ID that controller was "
                 "constructed with (${controller_chain_id})",
                 ("snapshot_chain_id", gpo.chain_id)("controller_chain_id", chain_id));
   }

   template <typename Section>
   void chainbase_add_contract_tables_to_snapshot(const chainbase::database& db, Section& section) {
      index_utils<table_id_multi_index>::walk(db, [&db, &section](const table_id_object& table_row) {
         // add a row for the table
         section.add_row(table_row, db);

         // followed by a size row and then N data rows for each type of table
         contract_database_index_set::walk_indices([&db, &section, &table_row](auto utils) {
            using utils_t     = decltype(utils);
            using value_t     = typename utils_t::index_t::value_type;
            using by_table_id = object_to_table_id_tag_t<value_t>;

            auto tid_key      = std::make_tuple(table_row.id);
            auto next_tid_key = std::make_tuple(table_id_object::id_type(table_row.id._id + 1));

            unsigned_int size = utils_t::template size_range<by_table_id>(db, tid_key, next_tid_key);
            section.add_row(size, db);

            utils_t::template walk_range<by_table_id>(db, tid_key, next_tid_key, [&db, &section](const auto& row) {
               section.add_row(row, db);
            });
         });
      });
   }

   void combined_database::add_contract_tables_to_snapshot(const snapshot_writer_ptr& snapshot) const {
      snapshot->write_section("contract_tables", [this](auto& section) {
         chainbase_add_contract_tables_to_snapshot(db, section);
      });
   }

   template <typename Section>
   void chainbase_read_contract_tables_from_snapshot(chainbase::database& db, Section& section) {
      bool more = !section.empty();
      while (more) {
         // read the row for the table
         table_id_object::id_type t_id;

         index_utils<table_id_multi_index>::create(db, [&db, &section, &t_id](auto& row) {
            section.read_row(row, db);
            t_id = row.id;
         });

         // read the size and data rows for each type of table
         contract_database_index_set::walk_indices([&db, &section, &t_id, &more](auto utils) {
            using utils_t = decltype(utils);

            unsigned_int size;
            more = section.read_row(size, db);

            for (size_t idx = 0; idx < size.value; ++idx) {
               utils_t::create(db, [&db, &section, &more, &t_id](auto& row) {
                  row.t_id = t_id;
                  more     = section.read_row(row, db);
               });
            }
         });
      }
   }

   void combined_database::read_contract_tables_from_snapshot(const snapshot_reader_ptr& snapshot) {
      snapshot->read_section("contract_tables", [this](auto& section) {
         chainbase_read_contract_tables_from_snapshot(db, section);
      });
   }

   std::optional<eosio::chain::genesis_state> extract_legacy_genesis_state(snapshot_reader& snapshot,
                                                                           uint32_t         version) {
      std::optional<eosio::chain::genesis_state> genesis;
      using v2 = legacy::snapshot_global_property_object_v2;

      if (std::clamp(version, v2::minimum_version, v2::maximum_version) == version) {
         genesis.emplace();
         snapshot.read_section<eosio::chain::genesis_state>(
               [&genesis = *genesis](auto& section) { section.read_row(genesis); });
      }
      return genesis;
   }

   // TODO : need to change this method to just return a char
   std::vector<char> make_rocksdb_contract_kv_prefix() { return std::vector<char> { backing_store::rocksdb_contract_kv_prefix }; }
   char make_rocksdb_contract_db_prefix() { return backing_store::rocksdb_contract_db_prefix; }

}} // namespace eosio::chain


