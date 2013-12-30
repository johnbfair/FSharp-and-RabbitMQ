open RabbitSample
open FSharp_RabbitMq.Types

let watch f name =
    let w = System.Diagnostics.Stopwatch.StartNew()
    f()
    w.Stop()
    printfn "Function %s took %i milliseconds to run" name w.ElapsedMilliseconds
    //System.Console.ReadLine() |> ignore

let processRabbit =
    printfn "Starting RabbitMQ processing"

    let count = 100000
    let host = {Host="amqp://localhost:5672"; Username="jfair2"; Password="admin"}
    
    let pub     = RabbitMqPublisher(host, Exchange "amq.direct")
    let msg1Pub = RabbitMqPublisher(host, Queue "message1")
    let msg2Pub = RabbitMqPublisher(host, Queue "message2")

    let sub     = RabbitMqSubscriber(host, "test")
    let msg1Sub = RabbitMqSubscriber(host, "message1")
    let msg2Sub = RabbitMqSubscriber(host, "message2")

    let grid = TheGrid(sub.AckMessage, msg1Pub, msg2Pub, count)

    sub.BindReceivedEvent (fun x y -> grid.RouteCommand(x,y))
    msg1Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage1Command (fun a -> msg1Sub.AckMessage a) (x,y))
    msg2Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage2Command (fun a -> msg2Sub.AckMessage a) (x,y))

    (fun _ -> 
        [1..count]
        |> List.map (fun i -> 
            if (i%2=0) then
                pub.Send "<Message1><Sample1>Hello world</Sample1><Value1>12345</Value1></Message1>"
            else
                pub.Send "<Message2><Sample2>dlrow olleH</Sample2><Value2>54321</Value2></Message2>")
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore) 
    |> watch <| "Publish"
    
    (fun _ -> sub.Start |> ignore) |> watch <| "Start Main Queue Processing"
    (fun _ -> msg1Sub.Start |> ignore) |> watch <| "Start Message1 Queue Processing"
    (fun _ -> msg2Sub.Start |> ignore) |> watch <| "Start Message2 Queue Processing"


[<EntryPoint>]
let main argv = 
    processRabbit
    0 // return an integer exit code