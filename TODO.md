# TODO:

- [ ] Durability & Atomicity through shadow-paging first
** write-lock the frame, copy page and acquire an upgradable
lock on the new frame. Swap the frame in the page_table.
Finish writing then upgrade lock, delete page and re-swap frame again **

but what if someone is waiting for the current frame's lock??

Ok, get an upgradable lock on the frame, copy page and acquire write lock
(not needed since no-one else should know that this page exists anyway)
write on second page and once completed upgrade first lock and swap the new page in + rename


- [ ] Durability through WAL
- [ ] Atomicity through WAL
- [ ] B+Trees
    - [ ] Schema constraints
    - [ ] Index pages
    - [ ] Algorithm itself
- [ ] Query Engine, no binder, no optimizer, just raw sql, plans, and execution
- [ ] Transactions and isolation MVCC

does the DM even need to know about transactions??? Yes
We need someone to hold the upgradable_locks, maybe a txn manager???
