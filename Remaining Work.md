# Remaining Work
- Handle large iterables:
  - [ ] Maximum videos to pull from channel
  - [ ] Maximum top-level comments to pull
  - [ ] Maximum comment replies to pull
- Handle duplicates: what happens if we run the script twice with the same channel? 


Execution Times:
  - Cloud Functions has 9 min max limit
  - In 103.664s, we processed: 
    - 5 videos, ~246 threads, ~320 comments/replies
    - 506 entity mentions across 490 entities
    - Table sizes:
      - comment_threads: 0.034194 MB
      - comments: 0.2194 MB
      - entities: 0.037901 MB
      - entity_mentions: 0.038152 MB
      - jobs: 0.000167 MB
      - videos: 0.011099 MB
      - Total: 0.340913 MB