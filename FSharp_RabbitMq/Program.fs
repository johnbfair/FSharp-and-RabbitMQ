open FSharp_RabbitMq.Types

let watch f name =
    let w = System.Diagnostics.Stopwatch.StartNew()
    f()
    w.Stop()
    printfn "Function %s took %i milliseconds to run" name w.ElapsedMilliseconds

let sleepytime time =
    printfn "Sleeping for %i milliseconds\n" time; System.Threading.Thread.Sleep time

let processRabbit_CBR(count) =
    printfn "Starting RabbitMQ with CBR processing"

    let host = {Host="amqp://localhost:5672"; Username="jfair2"; Password="admin"}
    
    let pub     = RabbitMqPublisher(host, Queue "cbr-main")
    let msg1Pub = RabbitMqPublisher(host, Queue "cbr-message1")
    let msg2Pub = RabbitMqPublisher(host, Queue "cbr-message2")

    let sub     = RabbitMqSubscriber(host, "cbr-main")
    let msg1Sub = RabbitMqSubscriber(host, "cbr-message1")
    let msg2Sub = RabbitMqSubscriber(host, "cbr-message2")

    let grid = RabbitCBRSample.TheGrid(sub.AckMessage, msg1Pub, msg2Pub, count)

    sub.BindReceivedEvent (fun x y -> grid.RouteCommand(x,y))
    msg1Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage1Command (fun a -> msg1Sub.AckMessage a) (x,y))
    msg2Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage2Command (fun a -> msg2Sub.AckMessage a) (x,y))

    (fun _ -> 
        [1..count]
        |> List.map (fun i -> 
            if (i%2=0) then
                pub.Send "<Message1><Sample1>Hello world</Sample1><Value1>12345</Value1></Message1>" None
            else
                pub.Send "<Message2><Sample2>dlrow olleH</Sample2><Value2>54321</Value2></Message2>" None)
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore) 
    |> watch <| "Publish"
    
    (fun _ -> sub.Start |> ignore) |> watch <| "Start Main Queue Processing"
    (fun _ -> msg1Sub.Start |> ignore) |> watch <| "Start Message1 Queue Processing"
    (fun _ -> msg2Sub.Start |> ignore) |> watch <| "Start Message2 Queue Processing"
    (fun _ -> grid.StartTimeKeeper()) |> watch <| "Start Accountant"
    
let processRabbit_Routing(count) =
    printfn "Starting RabbitMQ with Routing processing"

    let host = {Host="amqp://localhost:5672"; Username="jfair2"; Password="admin"}
    
    let pub     = RabbitMqPublisher(host, Exchange "amq.direct")

    let msg1Sub = RabbitMqSubscriber(host, "routing-message1")
    let msg2Sub = RabbitMqSubscriber(host, "routing-message2")

    let grid = RabbitRoutingSample.TheGrid(count)

    msg1Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage1Command (fun a -> msg1Sub.AckMessage a) (x,y))
    msg2Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage2Command (fun a -> msg2Sub.AckMessage a) (x,y))

    (fun _ -> 
        [1..count]
        |> List.map (fun i -> 
            if (i%2=0) then
                pub.Send "<Message1><Sample1>Hello world</Sample1><Value1>12345</Value1></Message1>" (Some "message1")
            else
                pub.Send "<Message2><Sample2>dlrow olleH</Sample2><Value2>54321</Value2></Message2>" (Some "message2"))
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore) 
    |> watch <| "Publish"
    
    (fun _ -> msg1Sub.Start |> ignore) |> watch <| "Start Message1 Queue Processing"
    (fun _ -> msg2Sub.Start |> ignore) |> watch <| "Start Message2 Queue Processing"
    (fun _ -> grid.StartTimeKeeper()) |> watch <| "Start Accountant"

[<EntryPoint>]
let main argv = 
    let rec loop() =
        printfn "Enter the number of Messages to process: "
        let count = System.Console.ReadLine() |> System.Int32.Parse
        let milliseconds = count * 100

        //processRabbit_CBR(count)
        
        printfn"Press enter to start the next test"; System.Console.ReadLine() |> ignore

        processRabbit_Routing(count)
        
        printfn"Press enter to restart the tests"; System.Console.ReadLine() |> ignore

        loop()
    loop()
    0 // return an integer exit code