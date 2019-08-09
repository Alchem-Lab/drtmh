// new implementation of TPC-C

#include "app/config.h"

#include "tpcc_worker.h"

namespace nocc {
namespace oltp {

extern __thread util::fast_random   *random_generator;

static std::map<int,int> *warehouse_hotmap = NULL;

namespace tpcc {

extern int g_new_order_remote_item_pct;

#if ENABLE_TXN_API

#ifdef CALVIN_TX
txn_result_t TpccWorker::txn_new_order_new_api(calvin_request *req, yield_func_t &yield) {

#if CHECKS
  if(worker_id_ != 0 || current_partition != 0)
    return txn_result_t(true,1);
#endif

#define MAX_ITEM 15

  struct new_order_req_info_t {
    uint warehouse_id;
    uint districtID;
    uint customerID;
    uint numItems;
    int item_id[MAX_ITEM];
    // uint is_item_local_bitmask;
    uint supplier_warehouse_id[MAX_ITEM];
    uint ol_quantity[MAX_ITEM];
  };

  static_assert(sizeof(new_order_req_info_t) < CALVIN_REQ_INFO_SIZE, "request info for new order is too large to fit into a calvin request!");

  const char* buf = req->req_info;

  // fprintf(stdout, "de-serialized txn inputs.\n");
  // fprintf(stdout, "warehouse_id = %u\n", ((new_order_req_info_t*)buf)->warehouse_id);
  // fprintf(stdout, "districtID = %u\n", ((new_order_req_info_t*)buf)->districtID);
  // fprintf(stdout, "customerID = %u\n", ((new_order_req_info_t*)buf)->customerID);
  // fprintf(stdout, "numItems = %u\n", ((new_order_req_info_t*)buf)->numItems);
  // for (int i = 0; i < ((new_order_req_info_t*)buf)->numItems; i++) {
  //   fprintf(stdout, "%-10d %-10u %-10u\n",
  //                   ((new_order_req_info_t*)buf)->item_id[i], 
  //                   // (((new_order_req_info_t*)buf)->is_item_local_bitmask & (1<<i)) != 0 ? "L" : "R", 
  //                   ((new_order_req_info_t*)buf)->supplier_warehouse_id[i],
  //                   ((new_order_req_info_t*)buf)->ol_quantity[i]
  //                   );
  // }

  //for testing: override the request info for testing purpose
  //the following test cases used to cause problems when read/writes are forwarded in the same batch.
//   ((new_order_req_info_t*)buf)->warehouse_id = 3;
//   ((new_order_req_info_t*)buf)->districtID = 1;
//   ((new_order_req_info_t*)buf)->customerID = 269;
//   ((new_order_req_info_t*)buf)->numItems = 15;
// int iid[MAX_ITEM] = {89831,
// 22247,
// 38631,
// 1087,
// 69287,
// 24286,
// 23263,
// 60130,
// 76383,
// 72413,
// 31325,
// 69027,
// 32167,
// 64037,
// 93877};
// uint wid[MAX_ITEM] = {
// 4,
// 1,
// 4,
// 1,
// 1,
// 2,
// 1,
// 2,
// 1,
// 2,
// 1,
// 2,
// 1,
// 2,
// 1};
// uint oq[MAX_ITEM] = {
// 9,
// 10,
// 2,
// 5,
// 5,
// 10,
// 6,
// 4,
// 1,
// 6,
// 2,
// 6,
// 4,
// 9,
// 2};


//another test case that is good:
//   ((new_order_req_info_t*)buf)->warehouse_id = 3;
//   ((new_order_req_info_t*)buf)->districtID = 10;
//   ((new_order_req_info_t*)buf)->customerID = 2306;
//   ((new_order_req_info_t*)buf)->numItems = 14;
// int iid[MAX_ITEM] = {89831,
// 30181,
// 11631,
// 79300,
// 12295,
// 567, 
// 3644,
// 89702,
// 15847,
// 46771,
// 87751,
// 48871,
// 24226,
// 98023,
// 48450};
// uint wid[MAX_ITEM] = {
// 5,
// 2,
// 1,
// 6,
// 2,
// 4,
// 1,
// 1,
// 5,
// 6,
// 2,
// 6,
// 2,
// 2};
// uint oq[MAX_ITEM] = {
// 3,
// 9,
// 3,
// 1,
// 4,
// 10,
// 10,
// 4,
// 1,
// 3,
// 8,
// 7,
// 1,
// 3};

// for (int i = 0; i < ((new_order_req_info_t*)buf)->numItems; i++) {
//     ((new_order_req_info_t*)buf)->item_id[i] = iid[i];
//     ((new_order_req_info_t*)buf)->supplier_warehouse_id[i] = wid[i];;
//     ((new_order_req_info_t*)buf)->ol_quantity[i] = oq[i];
// }

rtx_->begin(yield);

  const uint warehouse_id = ((new_order_req_info_t*)buf)->warehouse_id;
  const uint districtID = ((new_order_req_info_t*)buf)->districtID;
  const uint customerID = ((new_order_req_info_t*)buf)->customerID;
  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

  bool allLocal = true;
  std::set<uint64_t> stock_set; //remove identity stock ids;

  // local buffer used store stocks
  uint64_t remote_stocks[MAX_ITEM], local_stocks[MAX_ITEM];
  int remote_item_ids[MAX_ITEM],local_item_ids[MAX_ITEM];
  uint local_supplies[MAX_ITEM],remote_supplies[MAX_ITEM];

  int num_remote_stocks(0),num_local_stocks(0);

  const uint numItems = ((new_order_req_info_t*)buf)->numItems;
  for (uint i = 0; i < numItems; i++) {
    bool conflict = false;
    int item_id = ((new_order_req_info_t*)buf)->item_id[i];
    if (NumWarehouses() == 1 ||
        // (((new_order_req_info_t*)buf)->is_item_local_bitmask & (1<<i)) != 0)
      WarehouseToPartition(((new_order_req_info_t*)buf)->supplier_warehouse_id[i]) == warehouse_id)
    {
      // locla stock case
      uint supplier_warehouse_id = ((new_order_req_info_t*)buf)->supplier_warehouse_id[i];
      uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
      assert (stock_set.find(s_key) == stock_set.end());
      stock_set.insert(s_key);
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      allLocal = false;

      uint supplier_warehouse_id = ((new_order_req_info_t*)buf)->supplier_warehouse_id[i];
      uint64_t s_key = makeStockKey(supplier_warehouse_id, item_id);
      assert (stock_set.find(s_key) == stock_set.end());
      stock_set.insert(s_key);

      /* if possible, add remote stock to remote stocks */
      assert(WarehouseToPartition(supplier_warehouse_id) != warehouse_id);
      remote_stocks[num_remote_stocks] = s_key;
      remote_supplies[num_remote_stocks] = supplier_warehouse_id;
      remote_item_ids[num_remote_stocks++] = item_id;
    }
  }

  // Generating Read set / Write set phase ////////////////////////////////////////////////////
  uint64_t d_key = makeDistrictKey(warehouse_id,districtID);
  auto d_value_idx = rtx_->write<DIST,district::value>(WarehouseToPartition(warehouse_id),d_key,yield);
  if(d_value_idx == -1) return txn_result_t(false,73);

  std::vector<int> read_idx_local, read_idx_remote;
  std::vector<int> write_idx_local, write_idx_remote;
  std::vector<item::value*> read_local, read_remote;
  std::vector<stock::value*> write_local, write_remote;

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
    // const uint ol_i_id = local_item_ids[ol_number - 1];
    // auto idx = rtx_->read<ITEM,item::value>(WarehouseToPartition(warehouse_id),ol_i_id,yield);
    // if(idx == -1) return txn_result_t(false,73);
    // read_idx_local.push_back(idx);

    uint64_t s_key = local_stocks[ol_number  - 1];
    auto idx = rtx_->write<STOC,stock::value>(WarehouseToPartition(stockKeyToWare(s_key)),s_key,yield);
    if(idx == -1) return txn_result_t(false,73);
    write_idx_local.push_back(idx);
  }

  for(uint i = 0;i < num_remote_stocks;++i) {
    // const uint ol_i_id = remote_item_ids[i];
    // auto idx = rtx_->read<ITEM,item::value>(WarehouseToPartition(warehouse_id),ol_i_id,yield);
    // if(idx == -1) return txn_result_t(false,73);
    // read_idx_remote.push_back(idx);

    uint64_t s_key = remote_stocks[i];
    auto idx = rtx_->write<STOC,stock::value>(WarehouseToPartition(stockKeyToWare(s_key)),s_key,yield);
    if(idx == -1) return txn_result_t(false,73);
    write_idx_remote.push_back(idx);
  }

  // show read/write sets;
  // rtx_->display_sets();

  if (!rtx_->request_locks(yield)) {
    return txn_result_t(false,73);
  }

  // fprintf(stdout, "After local load_reads and local load_writes for %d:\n", req->req_seq);
  district::value *d_value = rtx_->get_writeset<district::value>(d_value_idx,yield);
  // fprintf(stdout, "d_value addr %p\n", d_value);
  
  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
    // item::value *i_value = rtx_->get_readset<item::value>(read_idx_local[ol_number-1],yield);
    item::value* i_value = new item::value;
    // fprintf(stdout, "i_value addr %p\n", i_value);
    read_local.push_back(i_value);

    stock::value *s_value = rtx_->get_writeset<stock::value>(write_idx_local[ol_number-1],yield);
    // fprintf(stdout, "s_value addr %p\n", s_value);
    write_local.push_back(s_value);  
  }

  for(uint i = 0;i < num_remote_stocks;++i) {
    // item::value *i_value = rtx_->get_readset<item::value>(read_idx_remote[i],yield);
    item::value* i_value = new item::value;
    // fprintf(stdout, "i_value addr %p\n", i_value);
    read_remote.push_back(i_value);

    stock::value *s_value = rtx_->get_writeset<stock::value>(write_idx_remote[i],yield);
    // fprintf(stdout, "s_value addr %p\n", s_value);
    write_remote.push_back(s_value); 
  }

  // show read/write sets;
  // rtx_->display_sets();
  
  if(!rtx_->sync_reads(req->req_seq, yield))
    return txn_result_t(true, 73);

  // fprintf(stdout, "%d read write synced.\n", req->req_seq);
  d_value = rtx_->get_writeset<district::value>(d_value_idx,yield);
  assert(d_value != NULL);

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
    // item::value *i_value = rtx_->get_readset<item::value>(read_idx_local[ol_number-1],yield);
    item::value* i_value = new item::value;
    assert(i_value != NULL);
    read_local[ol_number-1] = i_value;

    stock::value *s_value = rtx_->get_writeset<stock::value>(write_idx_local[ol_number-1],yield);
    assert(s_value != NULL);
    write_local[ol_number-1] = s_value;  
  }

  for(uint i = 0;i < num_remote_stocks;++i) {
    // item::value *i_value = rtx_->get_readset<item::value>(read_idx_remote[i],yield);
    item::value* i_value = new item::value;
    assert(i_value != NULL);
    read_remote[i] = i_value;
    stock::value *s_value = rtx_->get_writeset<stock::value>(write_idx_remote[i],yield);
    assert(s_value != NULL);
    write_remote[i] = s_value;
  }

  // execution phase
  ////////////////////////////////////////////////////
  // fprintf(stdout, "%d starts execution.\n", req->req_seq);
  const auto my_next_o_id = d_value->d_next_o_id;
  d_value->d_next_o_id ++;

  // first insert
  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;
  rtx_->insert<NEWO,new_order::value>(WarehouseToPartition(warehouse_id),no_key,&v_no,yield);

//   // second insert
  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);
  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();
  rtx_->insert<ORDE,oorder::value>(WarehouseToPartition(warehouse_id),o_key,(&v_oo),yield); // TODO!!

//   // third insert
  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;
  rtx_->insert<ORDER_INDEX,order_index_type_t>(WarehouseToPartition(warehouse_id),o_sec,(order_index_type_t *)array_dummy,yield);

  START(read);

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    item::value *i_value = read_local[ol_number-1];
    uint64_t s_key = local_stocks[ol_number-1];
    stock::value *s_value = write_local[ol_number-1];

    const uint ol_quantity = ((new_order_req_info_t*)buf)->ol_quantity[ol_number-1];
    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    rtx_->insert<ORLI,order_line::value>(WarehouseToPartition(warehouse_id),ol_key,(&v_ol),yield);
  }

//   /* operation remote objects */
  for(uint i = 0;i < num_remote_stocks;++i) {
    const uint ol_i_id = remote_item_ids[i];
    item::value *i_value = read_remote[i];
    uint64_t s_key = remote_stocks[i];
    stock::value *s_value = write_remote[i];

    const uint ol_quantity = ((new_order_req_info_t*)buf)->ol_quantity[num_local_stocks + i];
#if 1
#if CHECKS
    LOG(2) << "skey " << s_key << ",get remote stock, quantity " << s_value->s_quantity;
#endif
    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;
#if CHECKS
    LOG(2) << "modify quantity to " << s_value->s_quantity;
#endif
    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
#endif

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    rtx_->insert<ORLI,order_line::value>(WarehouseToPartition(warehouse_id),ol_key,(&v_ol),yield);
  }
  END(read);

  // fprintf(stdout, "%d starts to commit.\n", req->req_seq);
  bool res = rtx_->commit(yield);
  return txn_result_t(res,1);
}

#else

txn_result_t TpccWorker::txn_new_order_new_api(yield_func_t &yield) {

#if CHECKS
  if(worker_id_ != 0 || current_partition != 0)
    return txn_result_t(true,1);
#endif

  rtx_->begin(yield);

  // generate the req parameters ////////////////////////////////////////
  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  const uint districtID = RandomNumber(random_generator[cor_id_], 1, 10);
  //const uint districtID = 1;
  const uint customerID = GetCustomerId(random_generator[cor_id_]);
  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

#define MAX_ITEM 15

  bool allLocal = true;
  std::set<uint64_t> stock_set; //remove identity stock ids;

  // local buffer used store stocks
  uint64_t remote_stocks[MAX_ITEM];
  int remote_item_ids[MAX_ITEM],local_stocks[MAX_ITEM],local_item_ids[MAX_ITEM];
  uint local_supplies[MAX_ITEM],remote_supplies[MAX_ITEM];

  int num_remote_stocks(0),num_local_stocks(0);

  const uint numItems = RandomNumber(random_generator[cor_id_], 5, MAX_ITEM);
#if OR
  START(read);
#endif
  for (uint i = 0; i < numItems; i++) {
    bool conflict = false;
    int item_id = GetItemId(random_generator[cor_id_]);
    if (NumWarehouses() == 1 ||
        RandomNumber(random_generator[cor_id_], 1, 100) > g_new_order_remote_item_pct) {
      // locla stock case
      uint supplier_warehouse_id = warehouse_id;
      uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
      if(stock_set.find(s_key)!=stock_set.end()) {
        i--;
        continue;
      } else {
        stock_set.insert(s_key);
      }
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      // remote stock case
      uint64_t s_key;
      uint supplier_warehouse_id;
      do {
        supplier_warehouse_id = RandomNumber(random_generator[cor_id_], 1, NumWarehouses());
        s_key = makeStockKey(supplier_warehouse_id, item_id);
        if(stock_set.find(s_key)!=stock_set.end()){
          conflict = true;
        } else {
          stock_set.insert(s_key);
        }
      } while (supplier_warehouse_id == warehouse_id);
      allLocal =false;

      if(conflict){
        i--;
        continue;
      }
      /* if possible, add remote stock to remote stocks */
      if(WarehouseToPartition(supplier_warehouse_id) != current_partition) {
        remote_stocks[num_remote_stocks] = s_key;
        remote_supplies[num_remote_stocks] = supplier_warehouse_id;
        remote_item_ids[num_remote_stocks++] = item_id;
        // naive version will not do remote reads here

#if OR  // issue the reads here
        auto idx = rtx_->pending_read<STOC,stock::value>(WarehouseToPartition(supplier_warehouse_id),s_key,yield);
        rtx_->add_to_write();
#endif
      } else {
        local_item_ids[num_local_stocks] = item_id;
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_stocks[num_local_stocks++] = s_key;
      }
    }
  }
  // Execution phase ////////////////////////////////////////////////////
  // rtx_->read<CUST,customer::value>(current_partition,c_key,yield);
  // rtx_->read<WARE,warehouse::value>(current_partition,warehouse_id,yield);

  uint64_t d_key = makeDistrictKey(warehouse_id,districtID);
  auto idx = rtx_->write<DIST,district::value>(current_partition,d_key,yield);
  if(idx == -1) return txn_result_t(false,73);
  district::value *d_value = rtx_->get_writeset<district::value>(idx,yield);

  const auto my_next_o_id = d_value->d_next_o_id;
  d_value->d_next_o_id ++;

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;
  rtx_->insert<NEWO,new_order::value>(current_partition,no_key,&v_no,yield);

  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  rtx_->insert<ORDE,oorder::value>(current_partition,o_key,(&v_oo),yield); // TODO!!

  uint64_t *idx_value;

  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;

  rtx_->insert<ORDER_INDEX,order_index_type_t>(current_partition,o_sec,(order_index_type_t *)array_dummy,yield);

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    // auto idx = rtx_->read<ITEM,item::value>(current_partition,ol_i_id,yield);
    // item::value *i_value = rtx_->get_readset<item::value>(idx,yield);
    item::value* i_value = new item::value;

    uint64_t s_key = local_stocks[ol_number  - 1];

    idx = rtx_->write<STOC,stock::value>(current_partition,s_key,yield);
    if(idx == -1) return txn_result_t(false,73);
    stock::value *s_value = rtx_->get_writeset<stock::value>(idx,yield);

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    rtx_->insert<ORLI,order_line::value>(current_partition,ol_key,(&v_ol),yield);
  }

#if OR // fetch remote records
  indirect_yield(yield);
  END(read);
#endif

#if !OR
  START(read);
#endif
  /* operation remote objects */
  for(uint i = 0;i < num_remote_stocks;++i) {

    const uint ol_i_id = remote_item_ids[i];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    // auto idx = rtx_->read<ITEM,item::value>(current_partition,ol_i_id,yield);
    // item::value *i_value = rtx_->get_readset<item::value>(idx,yield);
    item::value *i_value = new item::value;

    uint64_t s_key = remote_stocks[i];

    // fetch remote objects
    // parse the result
#if OR
    idx = i;
    stock::value *s_value = rtx_->get_writeset<stock::value>(idx,yield);
#else
    idx = rtx_->write<STOC,stock::value>(WarehouseToPartition(stockKeyToWare(s_key)),s_key,yield);
    if(idx == -1) return txn_result_t(false,73);
    stock::value *s_value = rtx_->get_writeset<stock::value>(idx,yield);
#endif
    assert(s_value != NULL);
#if 1
#if CHECKS
    LOG(2) << "skey " << s_key << ",get remote stock, quantity " << s_value->s_quantity;
#endif
    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;
#if CHECKS
    LOG(2) << "modify quantity to " << s_value->s_quantity;
#endif
    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
#endif

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    rtx_->insert<ORLI,order_line::value>(current_partition,ol_key,(&v_ol),yield);
  }
#if !OR
  END(read);
#endif
  bool res = rtx_->commit(yield);
#if CHECKS
  // some checks
  rtx_->begin(yield);
  for(uint i = 0;i < num_remote_stocks;++i) {
    uint64_t s_key = remote_stocks[i];
    stock::value *s = rtx_->get<STOC,stock::value>(WarehouseToPartition(stockKeyToWare(s_key)),s_key,yield);
    LOG(2) << "recheck skey " << s_key << ",get remote stock, quantity " << s->s_quantity;
  }
  sleep(1);
#endif
  return txn_result_t(res,1);
}

#endif // CALVIN_TX

#ifdef CALVIN_TX
void TpccWorker::txn_new_order_new_api_gen_rwsets(char* buf, yield_func_t &yield) {
#define MAX_ITEM 15

  struct new_order_req_info_t {
    uint warehouse_id;
    uint districtID;
    uint customerID;
    uint numItems;
    int item_id[MAX_ITEM];
    // uint is_item_local_bitmask;
    uint supplier_warehouse_id[MAX_ITEM];
    uint ol_quantity[MAX_ITEM];
  };

  static_assert(sizeof(new_order_req_info_t) < CALVIN_REQ_INFO_SIZE, "request info for new order is too large to fit into a calvin request!");
  
  static_assert(MAX_ITEM >= 5, "MAX_ITEM less than 5 causes wired (maybe buffer override) problems!");

  // generate the req parameters ////////////////////////////////////////
  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  ((new_order_req_info_t*)buf)->warehouse_id = warehouse_id;

  const uint districtID = RandomNumber(random_generator[cor_id_], 1, 10);
  ((new_order_req_info_t*)buf)->districtID = districtID;

  const uint customerID = GetCustomerId(random_generator[cor_id_]);
  ((new_order_req_info_t*)buf)->customerID = customerID;

  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

  bool allLocal = true;
  std::set<uint64_t> stock_set; //remove identity stock ids;

  // local buffer used store stocks
  uint64_t remote_stocks[MAX_ITEM], local_stocks[MAX_ITEM];
  int remote_item_ids[MAX_ITEM],local_item_ids[MAX_ITEM];
  uint local_supplies[MAX_ITEM],remote_supplies[MAX_ITEM];

  int num_remote_stocks(0),num_local_stocks(0);

  const uint numItems = RandomNumber(random_generator[cor_id_], 5, MAX_ITEM);
  ((new_order_req_info_t*)buf)->numItems = numItems;
  // fprintf(stdout, "= numItems = %u\n", numItems);

  for (uint i = 0; i < numItems; i++) {
    bool conflict = false;
    int item_id = GetItemId(random_generator[cor_id_]);
    ((new_order_req_info_t*)buf)->item_id[i] = item_id;

    if (NumWarehouses() == 1 ||
        RandomNumber(random_generator[cor_id_], 1, 100) > g_new_order_remote_item_pct) {
      // locla stock case
      uint supplier_warehouse_id = warehouse_id;
      uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
      if(stock_set.find(s_key)!=stock_set.end()) {
        i--;
        continue;
      } else {
        stock_set.insert(s_key);
      }
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_stocks[num_local_stocks++] = s_key;
      // ((new_order_req_info_t*)buf)->is_item_local_bitmask |= (1<<i);
      assert(supplier_warehouse_id == warehouse_id);
      ((new_order_req_info_t*)buf)->supplier_warehouse_id[i] = warehouse_id;
      // fprintf(stdout, "a local warehouse id %u\n",  warehouse_id);
      // fprintf(stdout, "i = %d aa warehouse id %u\n", i, ((new_order_req_info_t*)buf)->supplier_warehouse_id[i]);
    } else {
      // remote stock case
      uint64_t s_key;
      uint supplier_warehouse_id;
      do {
        supplier_warehouse_id = RandomNumber(random_generator[cor_id_], 1, NumWarehouses());
        s_key = makeStockKey(supplier_warehouse_id, item_id);
        if(stock_set.find(s_key)!=stock_set.end()){
          conflict = true;
        } else {
          stock_set.insert(s_key);
        }
      } while (supplier_warehouse_id == warehouse_id);
      allLocal =false;

      if(conflict){
        i--;
        continue;
      }
      /* if possible, add remote stock to remote stocks */
      if(WarehouseToPartition(supplier_warehouse_id) != current_partition) {
        remote_stocks[num_remote_stocks] = s_key;
        remote_supplies[num_remote_stocks] = supplier_warehouse_id;
        remote_item_ids[num_remote_stocks++] = item_id;
        // naive version will not do remote reads here

        // ((new_order_req_info_t*)buf)->is_item_local_bitmask &= ~(1<<i);
        ((new_order_req_info_t*)buf)->supplier_warehouse_id[i] = supplier_warehouse_id;
        // fprintf(stdout, "b warehouse id %u\n",  supplier_warehouse_id);
        // fprintf(stdout, "i = %d bb warehouse id %u\n", i, ((new_order_req_info_t*)buf)->supplier_warehouse_id[i]);
      } else {
        local_item_ids[num_local_stocks] = item_id;
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_stocks[num_local_stocks++] = s_key;

        // ((new_order_req_info_t*)buf)->is_item_local_bitmask |= (1<<i);
        ((new_order_req_info_t*)buf)->supplier_warehouse_id[i] = supplier_warehouse_id;
        // fprintf(stdout, "c warehouse id %u\n",  supplier_warehouse_id);
        // fprintf(stdout, "i = %d cc warehouse id %u\n", i, ((new_order_req_info_t*)buf)->supplier_warehouse_id[i]);
      }
    }
  }

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);
    ((new_order_req_info_t*)buf)->ol_quantity[ol_number-1] = ol_quantity;
  }

   // operation remote objects 
  for(uint i = 0;i < num_remote_stocks;++i) {
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);
    ((new_order_req_info_t*)buf)->ol_quantity[num_local_stocks+i] = ol_quantity;
  }

  // struct new_order_req_info_t {
  //   uint warehouse_id;
  //   uint districtID;
  //   uint customerID;
  //   uint numItems;
  //   int item_id[MAX_ITEM];
  //   // uint is_item_local_bitmask;
  //   uint supplier_warehouse_id[MAX_ITEM];
  //   uint ol_quantity[MAX_ITEM];
  // };

  // fprintf(stdout, "serialized txn inputs.\n");
  // fprintf(stdout, "warehouse_id = %u\n", ((new_order_req_info_t*)buf)->warehouse_id);
  // fprintf(stdout, "districtID = %u\n", ((new_order_req_info_t*)buf)->districtID);
  // fprintf(stdout, "customerID = %u\n", ((new_order_req_info_t*)buf)->customerID);
  // fprintf(stdout, "numItems = %u\n", ((new_order_req_info_t*)buf)->numItems);
  // for (int i = 0; i < ((new_order_req_info_t*)buf)->numItems; i++) {
  //   fprintf(stdout, "%-10d %-10u %-10u\n", 
  //                   ((new_order_req_info_t*)buf)->item_id[i], 
  //                   // (((new_order_req_info_t*)buf)->is_item_local_bitmask & (1<<i)) != 0 ? "L" : "R", 
  //                   ((new_order_req_info_t*)buf)->supplier_warehouse_id[i],
  //                   ((new_order_req_info_t*)buf)->ol_quantity[i]
  //                   );
  // }
  return;
}
#endif

#endif // ENABLE_TXN_API

} // namespace tpcc

} // namespace oltp

} // namespace nocc
