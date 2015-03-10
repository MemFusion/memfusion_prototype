﻿
module Seq // GroupWhen


/// Iterates over elements of the input sequence and groups adjacent elements.
/// A new group is started when the specified predicate holds about the element
/// of the sequence (and at the beginning of the iteration).
///
/// For example: 
///    Seq.groupWhen isOdd [3;3;2;4;1;2] = seq [[3]; [3; 2; 4]; [1; 2]]
let groupWhen f (input:seq<_>) = seq {
  use en = input.GetEnumerator()
  let running = ref true
  
  // Generate a group starting with the current element. Stops generating
  // when it founds element such that 'f en.Current' is 'true'
  let rec group() = 
    [ yield en.Current
      if en.MoveNext() then
        if not (f en.Current) then yield! group() 
      else running := false ]
  
  if en.MoveNext() then
    // While there are still elements, start a new group
    while running.Value do
      yield group() |> Seq.ofList }
