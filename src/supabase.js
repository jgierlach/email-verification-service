import { createClient } from '@supabase/supabase-js'

const url = process.env.SUPABASE_URL || ''
const key = process.env.SUPABASE_SERVICE_ROLE_KEY || ''

if (!url || !key) {
  // Don't throw at import time — endpoints that don't need Supabase should
  // keep working. Callers that require it check `supabaseEnabled` first.
  console.warn('[supabase] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing — batch runner disabled')
}

export const supabase = url && key ? createClient(url, key, { auth: { persistSession: false } }) : null
export const supabaseEnabled = !!supabase
