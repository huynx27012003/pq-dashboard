import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { environment } from '../../environments/environment';
import {
    ApiMeterInfo,
    StartLoopRequest,
    MeterLoopStreamEvent,
    StopLoopResponse,
    MeterLoopStatusResponse,
    AddMeterRequest,
    AddMeterResponse,
    UpdateMeterRequest,
    UpdateMeterResponse,
    DeleteMeterResponse,
    QueryDataByTimeRangeRequest,
    QueryDataByTimeRangeResponse,
    QueryLatestRequest,
    QueryLatestResponse,
    ReadProfileRequest,
    ReadProfileResponse,
    MonthlySummaryResponse,
    MonthlyBreakdownResponse,
    FaultWindowResponse,
    EnergyRole,
    EnergySource,
    EnergyItemListResponse
} from '../interfaces/api.interface';

@Injectable({
    providedIn: 'root'
})
export class MeterApiService {
    private http = inject(HttpClient);
    private apiUrl = environment.apiUrl;

    /**
     * Get all meters info
     * GET /api/get_all_meters_info
     */
    getAllMetersInfo(): Observable<ApiMeterInfo[]> {
        return this.http.get<ApiMeterInfo[]>(`${this.apiUrl}/get_all_meters_info`)
            .pipe(catchError(this.handleError));
    }

    /**
     * Start reading loop for specified meters
     * POST /api/read_and_save_meters_loop_streaming_status (SSE)
     */
    startReadingLoopStream(meterIds: number[], lastEventId?: string): Observable<MeterLoopStreamEvent> {
        return new Observable<MeterLoopStreamEvent>(observer => {
            const controller = new AbortController();
            const request: StartLoopRequest = { meters_id_list: meterIds };

            const readStream = async () => {
                try {
                    const headers: Record<string, string> = {
                        'Content-Type': 'application/json',
                        'Accept': 'text/event-stream',
                    };
                    if (lastEventId) {
                        headers['Last-Event-ID'] = lastEventId;
                    }

                    const response = await fetch(`${this.apiUrl}/read_and_save_meters_loop_streaming_status`, {
                        method: 'POST',
                        headers,
                        body: JSON.stringify(request),
                        signal: controller.signal,
                    });

                    if (!response.ok) {
                        throw new Error(`Server error: ${response.status}`);
                    }

                    if (!response.body) {
                        throw new Error('No response body for streaming request.');
                    }

                    const reader = response.body.getReader();
                    const decoder = new TextDecoder('utf-8');
                    let buffer = '';

                    const flushBuffer = (forceFlush = false) => {
                        let separatorIndex = buffer.indexOf('\n\n');
                        while (separatorIndex !== -1) {
                            const rawEvent = buffer.slice(0, separatorIndex);
                            buffer = buffer.slice(separatorIndex + 2);
                            this.parseSseEvent(rawEvent, observer);
                            separatorIndex = buffer.indexOf('\n\n');
                        }

                        if (forceFlush && buffer.trim().length > 0) {
                            this.parseSseEvent(buffer, observer);
                            buffer = '';
                        }
                    };

                    while (true) {
                        const { value, done } = await reader.read();
                        if (done) {
                            break;
                        }
                        buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
                        flushBuffer();
                    }

                    flushBuffer(true);
                    observer.complete();
                } catch (error) {
                    if (!controller.signal.aborted) {
                        observer.error(error);
                    }
                }
            };

            readStream();

            return () => {
                controller.abort();
            };
        });
    }

    /**
     * Stop reading loop
     * GET /api/read_and_save_meters_loop_stop
     */
    stopReadingLoop(): Observable<StopLoopResponse> {
        return this.http.get<StopLoopResponse>(`${this.apiUrl}/read_and_save_meters_loop_stop`)
            .pipe(catchError(this.handleError));
    }

    /**
     * Poll current meter loop status (no body needed)
     * GET /api/meter_loop_status
     */
    getMeterLoopStatus(): Observable<MeterLoopStatusResponse> {
        return this.http.get<MeterLoopStatusResponse>(`${this.apiUrl}/meter_loop_status`)
            .pipe(catchError(this.handleError));
    }

    /**
     * Add new meter
     * POST /api/add_meter
     */
    addMeter(data: AddMeterRequest): Observable<AddMeterResponse> {
        return this.http.post<AddMeterResponse>(`${this.apiUrl}/add_meter`, data)
            .pipe(catchError(this.handleError));
    }

    /**
     * Update existing meter
     * PUT /api/update_meter
     */
    updateMeter(data: UpdateMeterRequest): Observable<UpdateMeterResponse> {
        return this.http.put<UpdateMeterResponse>(`${this.apiUrl}/update_meter`, data)
            .pipe(catchError(this.handleError));
    }

    /**
     * Delete meter by ID
     * DELETE /api/delete_meter/{id}
     */
    deleteMeter(meterId: number): Observable<DeleteMeterResponse> {
        return this.http.delete<DeleteMeterResponse>(`${this.apiUrl}/delete_meter/${meterId}`)
            .pipe(catchError(this.handleError));
    }

    /**
     * Query meter data by time range
     * POST /api/query_data_by_time_range
     */
    queryDataByTimeRange(params: QueryDataByTimeRangeRequest): Observable<QueryDataByTimeRangeResponse> {
        return this.http.post<QueryDataByTimeRangeResponse>(`${this.apiUrl}/query_data_by_time_range`, params)
            .pipe(catchError(this.handleError));
    }

    /**
     * Query latest meter readings
     * POST /api/query_reading_latest
     */
    queryLatestReading(params: QueryLatestRequest): Observable<QueryLatestResponse> {
        return this.http.post<QueryLatestResponse>(`${this.apiUrl}/query_reading_latest`, params)
            .pipe(catchError(this.handleError));
    }

    /**
     * Read load profile data
     * POST /api/read_profile
     */
    readProfile(params: ReadProfileRequest): Observable<ReadProfileResponse> {
        return this.http.post<ReadProfileResponse>(`${this.apiUrl}/read_profile`, params)
            .pipe(catchError(this.handleError));
    }

    /**
     * Handle HTTP errors
     */
    private handleError(error: HttpErrorResponse): Observable<never> {
        let errorMessage = 'An error occurred';

        if (error.error instanceof ErrorEvent) {
            // Client-side error
            errorMessage = `Error: ${error.error.message}`;
        } else {
            // Server-side error
            if (error.status === 0) {
                errorMessage = 'Unable to connect to server. Please check your network connection.';
            } else {
                errorMessage = error.error?.detail || error.error?.status || `Server error: ${error.status}`;
            }
        }

        console.error('MeterApiService Error:', errorMessage, error);
        return throwError(() => new Error(errorMessage));
    }

    private parseSseEvent(rawEvent: string, observer: { next: (value: MeterLoopStreamEvent) => void }): void {
        const lines = rawEvent.split(/\r?\n/);
        let eventType: MeterLoopStreamEvent['event'] = 'status';
        let eventId: string | undefined;
        const dataLines: string[] = [];

        for (const line of lines) {
            if (line.startsWith(':')) {
                continue;
            }
            if (line.startsWith('event:')) {
                eventType = line.replace('event:', '').trim() as MeterLoopStreamEvent['event'];
                continue;
            }
            if (line.startsWith('id:')) {
                eventId = line.replace('id:', '').trim();
                continue;
            }
            if (line.startsWith('data:')) {
                dataLines.push(line.replace('data:', '').trim());
                continue;
            }
        }

        if (!dataLines.length) {
            return;
        }

        const dataText = dataLines.join('\n');
        try {
            const parsed = JSON.parse(dataText);
            observer.next({ event: eventType, data: parsed, id: eventId });
        } catch (error) {
            console.warn('Unable to parse SSE payload', error, dataText);
        }
    }

    /**
     * Get monthly energy summary
     * GET /api/energy/monthly-summary?year=YYYY&month=M
     */
    getMonthlySummary(year: number, month: number): Observable<MonthlySummaryResponse> {
        return this.http.get<MonthlySummaryResponse>(
            `${this.apiUrl}/energy/monthly-summary`,
            { params: { year: year.toString(), month: month.toString() } }
        ).pipe(catchError(this.handleError));
    }

    /**
     * Get monthly energy breakdown by period
     * GET /api/energy/monthly-breakdown?year=YYYY&month=M
     */
    getMonthlyBreakdown(year: number, month: number): Observable<MonthlyBreakdownResponse> {
        return this.http.get<MonthlyBreakdownResponse>(
            `${this.apiUrl}/energy/monthly-breakdown`,
            { params: { year: year.toString(), month: month.toString() } }
        ).pipe(catchError(this.handleError));
    }

    /**
     * Get faults during a specific time window (for periodic breakdown detail)
     * GET /api/faults/by-window
     */
    getFaultsByWindow(startTs: string, endTs: string): Observable<FaultWindowResponse> {
        return this.http.get<FaultWindowResponse>(
            `${this.apiUrl}/faults/by-window`,
            { params: { window_start_ts: startTs, window_end_ts: endTs } }
        ).pipe(catchError(this.handleError));
    }

    /**
     * Get all faults for a specific month (for history tab)
     * GET /api/faults/by-month
     */
    getFaultsByMonth(year: number, month: number): Observable<FaultWindowResponse[]> {
        return this.http.get<FaultWindowResponse[]>(
            `${this.apiUrl}/faults/by-month`,
            { params: { year: year.toString(), month: month.toString() } }
        ).pipe(catchError(this.handleError));
    }

    /**
     * Download interval raw data as CSV
     * GET /api/energy/interval-raw/csv
     */
    downloadIntervalRawCSV(fromTs: string, toTs: string): Observable<Blob> {
        return this.http.get(
            `${this.apiUrl}/energy/interval-raw/csv`,
            {
                params: { from_ts: fromTs, to_ts: toTs },
                responseType: 'blob'
            }
        ).pipe(catchError(this.handleError));
    }

    /**
     * Get energy roles
     * GET /api/energy/roles
     */
    getRoles(): Observable<EnergyItemListResponse<EnergyRole>> {
        return this.http.get<EnergyItemListResponse<EnergyRole>>(`${this.apiUrl}/energy/roles`)
            .pipe(catchError(this.handleError));
    }

    /**
     * Get energy sources
     * GET /api/energy/sources
     */
    getSources(): Observable<EnergyItemListResponse<EnergySource>> {
        return this.http.get<EnergyItemListResponse<EnergySource>>(`${this.apiUrl}/energy/sources`)
            .pipe(catchError(this.handleError));
    }
}

