open FSharp_RabbitMq.Types

//TODO: If you don't want to use the default RabbitMQ account then change it here
let host = {Host="amqp://localhost:5672"; Username="guest"; Password="guest"}

// Basic way to measure how long a function takes to execute, unless you're using Async/Agents
let watch f name =
    let w = System.Diagnostics.Stopwatch.StartNew()
    f()
    w.Stop()
    printfn "Function %s took %i milliseconds to run" name w.ElapsedMilliseconds

let publishMessages (pub:RabbitMqPublisher) routingKey count =
    [1..count]
    |> List.map (fun i -> 
        if (i%2=0) then
            // This is a hack so I can dynamically generate 2 different kinds of messages and add specific routing keys
            // Please don't ever do this.
            match routingKey with
            | Some x -> pub.Send "<Message1><Sample1>Hello world</Sample1><Value1>12345</Value1></Message1>" (Some <| x + "1")
            | _ -> pub.Send "<Message1><Sample1>Hello world</Sample1><Value1>12345</Value1></Message1>" None
        else
            match routingKey with
            | Some x -> pub.Send "<Message2><Sample2>dlrow olleH</Sample2><Value2>54321</Value2></Message2>" (Some <| x + "2")
            | _ -> pub.Send "<Message2><Sample2>dlrow olleH</Sample2><Value2>54321</Value2></Message2>" None)
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    pub.EnsureConfirms(100.)

let processRabbit_CBR count =
    printfn "Starting RabbitMQ with CBR processing"
    
    let pub     = RabbitMqPublisher(host, Queue "cbr-main")
    let msg1Pub = RabbitMqPublisher(host, Queue "cbr-message1")
    let msg2Pub = RabbitMqPublisher(host, Queue "cbr-message2")
    let sub     = RabbitMqSubscriber(host, "cbr-main")
    let msg1Sub = RabbitMqSubscriber(host, "cbr-message1")
    let msg2Sub = RabbitMqSubscriber(host, "cbr-message2")

    let grid = RabbitCBRSample.TheGrid(sub.AckMessage, msg1Pub, msg2Pub, count)

    // Since we're waiting on confirms we need to listen to the event that occurs if a confirm fails
    pub.BindNackEvent     (fun x y -> printfn "Error occurred while sending. Requeued? %A DeliveryTag: %i" x y)
    msg1Pub.BindNackEvent (fun x y -> printfn "Error occurred while sending. Requeued? %A DeliveryTag: %i" x y)
    msg2Pub.BindNackEvent (fun x y -> printfn "Error occurred while sending. Requeued? %A DeliveryTag: %i" x y)

    // Bind the event that fires when we receive a message from RabbitMQ
    sub.BindReceivedEvent     (fun x y -> grid.RouteCommand(x,y))
    msg1Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage1Command (fun a -> msg1Sub.AckMessage a) (x,y))
    msg2Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage2Command (fun a -> msg2Sub.AckMessage a) (x,y))

    // Trigger message send
    (fun _ -> publishMessages pub None count) |> watch <| sprintf "Sending %i Messages" count

    // Start Timing
    grid.StartTimeKeeper()
        
    // Start the listeners
    sub.Start |> ignore
    msg1Sub.Start |> ignore
    msg2Sub.Start |> ignore
    
let processRabbit_Routing count =
    printfn "Starting RabbitMQ with Routing processing"
    
    let pub     = RabbitMqPublisher(host, Exchange "amq.direct")
    let msg1Sub = RabbitMqSubscriber(host, "routing-message1")
    let msg2Sub = RabbitMqSubscriber(host, "routing-message2")

    let grid = RabbitRoutingSample.TheGrid(count)

    // Since we're waiting on confirms we need to listen to the event that occurs if a confirm fails
    pub.BindNackEvent     (fun x y -> printfn "Error occurred while sending. Requeued? %A DeliveryTag: %i" x y)

    // Bind the event that fires when we receive a message from RabbitMQ
    msg1Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage1Command (fun a -> msg1Sub.AckMessage a) (x,y))
    msg2Sub.BindReceivedEvent (fun x y -> grid.ProcessMessage2Command (fun a -> msg2Sub.AckMessage a) (x,y))

    // Trigger message send
    (fun _ -> publishMessages pub (Some "message") count) |> watch <| sprintf "Sending %i Messages" count

    // Start Timing
    grid.StartTimeKeeper()

    // Start the listeners
    msg1Sub.Start |> ignore
    msg2Sub.Start |> ignore

[<EntryPoint>]
let main argv = 
    printfn "Enter the number of Messages to process: "
    
    // Don't be a jerk and enter letters...or do; it really doesn't matter to me. :)
    let count = System.Console.ReadLine() |> System.Int32.Parse
    
    processRabbit_CBR count
    
    printfn "\nPress enter to start the next test after the results of the first post here"; 
    System.Console.ReadLine() |> ignore

    processRabbit_Routing count

    System.Console.ReadLine() |> ignore
    0 // return an integer exit code