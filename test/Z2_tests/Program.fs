// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open MemFusion.DB
open MemFusionDB.BSONAst
open MemFusionDB.Z2
open MemFusionDB.Collection

let checkDoc (db :DB) doc print =
    let one = db.z2encode doc
    let two = (db.z2decode one)
    let pass = doc = two
    if print then
        printfn "\n\n%A\n\n" doc
        printfn "%A\n\n" one
        printfn "%A\n\n" two
        printf "Are the same?  %s" (if doc = two then "yes!" else "no")
    pass


[<EntryPoint>]
let main argv =
    let db = new DB("test")

    let doc1 = (0, [ UTF8String("version", (strOf "0.1.0"));
                    UTF8String("gitVersion", (strOf "552fe0d21959e32a5bdbecdc62057db386e4e029c"));
                    UTF8String("flavor", (strOf "MF"));
                    Int64("bits", 64L); Floatnum("ok", 1.0); ] )
    checkDoc db doc1 true |> ignore

    let doc1 = (0, [ BinaryData("somebin", (1uy, "ciaociao" |> System.Text.Encoding.ASCII.GetBytes));  ] )
    checkDoc db doc1 true |> ignore

    0 // return an integer exit code

