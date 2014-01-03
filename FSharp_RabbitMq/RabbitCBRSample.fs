module RabbitCBRSample

    open System
    open System.Xml
    open System.Xml.Linq
    open FSharp_RabbitMq.Types

    type TheGrid(ack, message1CmdQ: RabbitMqPublisher, message2CmdQ: RabbitMqPublisher, count:int) =        
        let watch = new System.Diagnostics.Stopwatch()
        
        // Due to the way XSerializer caches the default serializer we're better off
        // newing them up a single time and using the cached instance (for perf)
        let message1Serializer = lazy XSerializer.XmlSerializer<Message1>()
        let message2Serializer = lazy XSerializer.XmlSerializer<Message2>()

        // Error handling supervisor
        let supervisor = new Agent<System.Exception>(fun inbox ->
                            let rec Loop() =
                                async {
                                    let! err = inbox.Receive()
                                    printfn "An error occurred: %s" err.Message
                                    do! Loop() }
                            Loop()) |> Agent.start

        // Timekeeper is responsible for starting/stopping the stopwatch that determines how long
        // it takes to execute *count* messages.
        let timeKeeper = 
            new Agent<Status>(fun inbox ->
                let rec Loop() =
                    async {
                        let! msg = inbox.Receive()

                        match msg with
                        | Start -> watch.Start(); do! Loop()
                        | Stop -> 
                            watch.Stop()
                            printfn "Rabbit CBR processed %f messages/sec. Total Seconds: %f" ((float)count/watch.Elapsed.TotalSeconds) watch.Elapsed.TotalSeconds 
                        }
                Loop()) |> Agent.start

        // The Accountant is responsible for keeping track of how many messages have been processed so far out of
        // *count* messages. Once we reach zero (counting backwards) it issues the Stop command to the Timekeeper
        let accountant = 
            new Agent<_>(fun inbox ->
                let rec Loop totalCount =
                    async {
                        let! msg = inbox.Receive()
                        
                        let totalCount' = totalCount - 1
                        
                        if (totalCount' = 0) then 
                            timeKeeper.Post Stop

                        do! Loop totalCount' }
                // Double the count due to this type of processing (CBR)
                Loop (count*2)) |> Agent.start
                     
        member this.StartTimeKeeper() = timeKeeper.Post Start

        // This takes the initial message off the queue, determines its type, and sends it to the appropriate queue
        member this.RouteCommand(extMsg:string, tag:uint64) =
            (new Agent<Command>(fun inbox ->
                async {
                        let! msg = inbox.Receive()
                        // Use the RegExes to figure out what kind of message we have. This is typically how CBR is done
                        // but inside of the ESB/MQ
                        let msgType =
                            match System.Text.RegularExpressions.Regex.Match(msg.Message, "<(?<tag>\w*)>") with
                            | x when x.Success -> x.Groups.[1].Value
                            | _ -> raise (new Exception "Couldn't determine message type (null root)")

                        match msgType.ToLower() with
                        | "message1" ->                           
                            (new Agent<Command>(fun inbox ->
                                                    async {
                                                        let! x = inbox.Receive()
                                                        let! m = message1CmdQ.Send x.Message None
                                                        ack x.Tag
                                                    }) |> Agent.reportErrorsTo supervisor |> Agent.start) 
                            |> fun x-> x.Post msg

                        | "message2" -> 
                            (new Agent<Command>(fun inbox ->
                                                    async {
                                                        let! x = inbox.Receive()
                                                        let! m = message2CmdQ.Send x.Message None
                                                        ack x.Tag
                                                    }) |> Agent.reportErrorsTo supervisor |> Agent.start) 
                            |> fun x-> x.Post msg
                        | _ -> ack msg.Tag; raise (new Exception "Invalid Command") 
                        
                        // Tell the account that this message is complete
                        accountant.Post()
                        })
                |> Agent.reportErrorsTo supervisor 
                |> Agent.start)
                |> fun x-> x.Post {Message=extMsg; Tag=tag}

        member this.ProcessMessage1Command ack (extMsg:string, tag:uint64) =
            (new Agent<Message1>(fun inbox ->
                async {
                    let! msg = inbox.Receive()
                    ack msg.MessageTag

                    accountant.Post()
                })
            |> Agent.reportErrorsTo supervisor 
            |> Agent.start)
            |> fun x-> 
                // Because we're doing our own Content Based Routing we know the type explicitly (i.e. no try/with)
                let msg = message1Serializer.Value.Deserialize(extMsg)
                msg.MessageTag <- tag
                msg |> x.Post

        member this.ProcessMessage2Command ack (extMsg:string, tag:uint64) =
            (new Agent<Message2>(fun inbox ->
                async {
                    let! msg = inbox.Receive()
                    ack msg.MessageTag

                    accountant.Post()
                })
            |> Agent.reportErrorsTo supervisor 
            |> Agent.start)
            |> fun x-> 
                // Because we're doing our own Content Based Routing we know the type explicitly (i.e. no try/with)
                let msg = message2Serializer.Value.Deserialize(extMsg) 
                msg.MessageTag <- tag
                msg |> x.Post

        interface IDisposable with
            member this.Dispose() = 
                timeKeeper |> Agent.stop
                accountant |> Agent.stop
                supervisor |> Agent.stop