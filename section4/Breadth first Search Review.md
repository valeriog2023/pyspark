Scenario:
Find degree of separation between different SuperHeros
SP: spinderman
IM: Iron Man
TH: Thor
HU: Hulk

And assume:
IM ---------TH
  \         /
   \       /
    \     /
     \   /
       HU -------- SM

IM, TH, HU at 1 degree of sepration and SM at 2 from IM and TH and 1 to HU
we basically need to find the min distance between 2 nodes in a graph

Let's start with
  - all nodes empty and marked with infinite distance (white/unprocessed)
--- first pass
  - pick a node, mark it gray and set distance to 0 (distance to iteself)
    if we pick HU: HU d=0,
--- second pass
    - mark the gray node black (completed processing)
    - mark as gray all nodes connected to that one
    - set their distance to 1 (black node distance + 1)
--- third pass
    - repeat as before, set the gray nodes as black
    - mark sa gray all nodes connected to the previous gray nodes
      (unless they are already black)
    - set their distance to previous gray node + 1


check degree-of-sepation.py