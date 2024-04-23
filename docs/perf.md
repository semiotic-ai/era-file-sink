# performance increase attempt

After running sink with `cargo run  -- ./out  0:10` there is the following output:

```________________________________________________________
Executed in   27,17 secs    fish           external
   usr time   25,93 secs  354,00 micros   25,93 secs
   sys time    1,22 secs  208,00 micros    1,22 secs
```

One way to make it faster is making concurrent requests. They are evidently not concurrent because `.era1` files arrive
one by one in order in the `out` folder. Since they are independent from each other, they can be fetched concurrently. 

One thing to notice is that passing `0:10` as argument is fetching eras from 0 to 11.


This below is the time when not doing transformations on the data arriving from the streamingFast API:

```
________________________________________________________
Executed in   23,01 secs    fish           external
   usr time   20,91 secs  301,00 micros   20,91 secs
   sys time    1,14 secs  184,00 micros    1,14 secs
```

This indicates that the problem is mostly the sequential fetch of the data, not it's processing.