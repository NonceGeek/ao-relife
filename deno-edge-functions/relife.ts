import { Application, Router } from "https://deno.land/x/oak/mod.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { oakCors } from "https://deno.land/x/cors/mod.ts";

const router = new Router();

router.get("/event", async (context) => {
  const queryParams = context.request.url.searchParams;
  const uniqueId = queryParams.get("unique_id");
  const eventNum = queryParams.get("event_num");

  // Initialize Supabase client
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL") ?? "",
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  );

  if (!uniqueId || !eventNum) {
    context.response.status = 400;
    context.response.body = {
      error: "unique_id and event_num parameters are required",
    };
    return;
  }

  try {
    // Fetch the record from Supabase
    const { data, error } = await supabase
      .from("sim_life_events_v2")
      .select(`event_${eventNum}`)
      .eq("unique_id", uniqueId)
      .single();

    if (error) {
      throw error;
    }

    if (!data) {
      context.response.status = 404;
      context.response.body = { error: "Record not found" };
      return;
    }

    const event = data[`event_${eventNum}`];
    context.response.body = { event: event };
  } catch (error) {
    context.response.status = 500;
    context.response.body = {
      error: "An error occurred while fetching the record",
    };
  }
});


const app = new Application();
app.use(oakCors()); // Enable CORS for All Routes
app.use(router.routes());

console.info("CORS-enabled web server listening on port 8000");
await app.listen({ port: 8000 });
