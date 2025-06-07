// src/app/api/graphdb/route.ts
import axios, { AxiosError, AxiosResponse } from 'axios';
import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

interface SparqlBinding {
  [variable: string]: {
    type: 'uri' | 'literal' | 'bnode';
    value: string;
    datatype?: string;
    'xml:lang'?: string;
  };
}

interface SparqlResults {
  head: {
    vars: string[];
  };
  results: {
    bindings: SparqlBinding[];
  };
}

interface GraphDBErrorResponse {
  message: string;
  details?: unknown; // Changed to unknown to accept any error details
}

interface RequestBody {
  query: string;
}

export async function POST(request: NextRequest): Promise<NextResponse<SparqlResults | GraphDBErrorResponse>> {
  const graphDBEndpoint = 'http://localhost:7200/repositories/letstalk';

  let requestBody: RequestBody;
  try {
    requestBody = await request.json();
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Invalid request body';
    return NextResponse.json(
      { message: 'Invalid JSON body', details: errorMessage },
      { status: 400 }
    );
  }

  if (!requestBody.query) {
    return NextResponse.json(
      { message: 'Missing SPARQL query in request body' },
      { status: 400 }
    );
  }

  try {
    const response: AxiosResponse<SparqlResults> = await axios.post(
      graphDBEndpoint,
      `query=${encodeURIComponent(requestBody.query)}`,
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/sparql-results+json',
        },
      }
    );

    return NextResponse.json(response.data, { status: response.status });

  } catch (error: unknown) {
    const axiosError = error as AxiosError;
    console.error('Error querying GraphDB:', axiosError.message);

    let status = 500;
    let message = 'Error sending query to GraphDB';
    let details: unknown = 'Unknown error';

    if (axiosError.response) {
      status = axiosError.response.status;
      message = 'Error from GraphDB';
      details = axiosError.response.data;
      
      console.error('GraphDB response:', {
        status: axiosError.response.status,
        data: axiosError.response.data,
        headers: axiosError.response.headers,
      });
    } else if (axiosError.request) {
      message = 'No response received from GraphDB';
      details = 'The request was made but no response was received';
      console.error('No response received:', axiosError.request);
    } else if (axiosError.message) {
      details = axiosError.message;
    }

    return NextResponse.json(
      { message, details },
      { status }
    );
  }
}