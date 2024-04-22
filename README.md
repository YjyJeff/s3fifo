# S3FIFO
Implementation of the [`S3FIFO`](https://dl.acm.org/doi/10.1145/3600006.3613147) algorithm presented in SOSP 2023.

# Tittle-tattle

## Ghost queue
How to implement the ghost queue? 

## Efficiency of the LRU and S3FIFO
In my view, LRU is efficient because **linked lists are the defacto heroes of the dark world of lock-free concurrency**. In some cases, S3FIFO may still need linked lists because the capacity of the cache is unknown. Author of the paper also write a [blog](https://blog.jasony.me/system/cache/2023/12/28/fifo) to describe how to implement a FIFO queue with linked list. They point out the disadvantage of the linked list and still using it 😂


# TODO
- [ ] Thread safe
- [ ] Evaluation