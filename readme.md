### TODOs:

- [x] eliminate unnecessary data (ix/tx)
- [ ] Better way to record the "called-to" bytes in programs (quadratically grwoing `data_first_byte` table rn)
- [x] Error handling and loggin
- [ ] Abstract out the processing(db calls) from the infra(threading)

epoch is 432,000 slots, each of which should at a minimum take 400ms. 