import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatTabsModule } from '@angular/material/tabs';
import { MatTableModule } from '@angular/material/table';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatNativeDateModule } from '@angular/material/core';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatDividerModule } from '@angular/material/divider';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration, ChartType, Chart } from 'chart.js';
import zoomPlugin from 'chartjs-plugin-zoom';
import 'hammerjs';

Chart.register(zoomPlugin);

import { MeterService } from '../../services/meter.service';
import { MeterApiService } from '../../services/meter-api.service';
import { MeterDetail, MeterOperatingParams } from '../../interfaces/meter.interface';
import { MeterDataPoint, COLUMN_DEFINITIONS, ColumnDefinition } from '../../interfaces/api.interface';
import * as XLSX from 'xlsx';

@Component({
    selector: 'app-meter-detail',
    standalone: true,
    imports: [
        CommonModule,
        ReactiveFormsModule,
        MatCardModule,
        MatTabsModule,
        MatTableModule,
        MatIconModule,
        MatButtonModule,
        MatProgressSpinnerModule,
        MatFormFieldModule,
        MatDatepickerModule,
        MatNativeDateModule,
        MatInputModule,
        MatSelectModule,
        MatDividerModule,
        MatTooltipModule,
        MatSnackBarModule,
        BaseChartDirective,
    ],
    templateUrl: './meter-detail.component.html',
    styleUrl: './meter-detail.component.scss',
})
export class MeterDetailComponent implements OnInit {
    private route = inject(ActivatedRoute);
    private router = inject(Router);
    private meterService = inject(MeterService);
    private meterApi = inject(MeterApiService);
    private snackBar = inject(MatSnackBar);

    meter: MeterDetail | null = null;
    isLoading = true;
    isLoadingData = false;
    selectedTabIndex = 0;
    meterId: number = 0;

    // API data
    apiDataPoints: MeterDataPoint[] = [];

    // Date filters
    fromDate = new FormControl(new Date(Date.now() - 24 * 60 * 60 * 1000)); // Last 24 hours
    toDate = new FormControl(new Date());
    fromTime = new FormControl('00:00');
    toTime = new FormControl(this.formatTimeValue(new Date()));

    // Selection controls
    columnControl = new FormControl<string[]>([
        'phase_a_voltage', 'phase_b_voltage', 'phase_c_voltage',
        'phase_a_current', 'phase_b_current', 'phase_c_current',
        'p_total', 'power_factor'
    ]);
    intervalControl = new FormControl<number>(60);
    loadSurveyControl = new FormControl<'LS02' | 'LS03'>('LS02');
    loadColumnControl = new FormControl<string[]>([]);

    // Available options
    allColumns = COLUMN_DEFINITIONS;
    loadProfileFields: string[] = [];
    intervals = [
        { label: '30s', value: 30 },
        { label: '1m', value: 60 },
        { label: '5m', value: 300 },
        { label: '15m', value: 900 },
        { label: '30m', value: 1800 },
        { label: '1h', value: 3600 },
    ];

    // Table columns
    operatingColumns: string[] = ['timestamp'];
    loadColumns: string[] = ['timestamp'];
    periodicColumns: string[] = ['timestamp', 'total_import_kwh', 'total_export_kwh', 'p_total', 'q_total'];
    finalizedColumns: string[] = ['timestamp', 'total_import_kwh', 'total_export_kwh'];

    // Chart 1: Voltage Chart
    operatingChartType: ChartType = 'line';
    operatingChartData: ChartConfiguration['data'] = { labels: [], datasets: [] };
    operatingChartOptions: ChartConfiguration['options'] = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'top' },
            zoom: {
                pan: {
                    enabled: true,
                    mode: 'x',
                },
                zoom: {
                    wheel: {
                        enabled: true,
                    },
                    pinch: {
                        enabled: true
                    },
                    mode: 'x',
                }
            }
        },
        scales: {
            y: {
                beginAtZero: false,
                title: { display: false },
            },
            x: {
                title: { display: true, text: 'Time' },
            },
        },
    };

    // Chart 2: Power Chart
    loadChartType: ChartType = 'line';
    loadChartData: ChartConfiguration['data'] = { labels: [], datasets: [] };
    loadChartOptions: ChartConfiguration['options'] = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'top' },
            zoom: {
                pan: {
                    enabled: true,
                    mode: 'x',
                },
                zoom: {
                    wheel: {
                        enabled: true,
                    },
                    pinch: {
                        enabled: true
                    },
                    mode: 'x',
                }
            }
        },
        scales: {
            y: {
                beginAtZero: false,
                title: { display: false },
            },
            x: {
                title: { display: true, text: 'Time' },
            },
        },
    };

    loadSurveyLabel = 'LS02';
    loadProfileDataPoints: MeterDataPoint[] = [];
    loadSamplingIntervalLabel = '';

    ngOnInit(): void {
        this.meterId = Number(this.route.snapshot.paramMap.get('id'));
        if (this.meterId) {
            this.loadMeterDetail(this.meterId);
            this.loadMeterData();
            this.loadProfileData();
        }
    }

    loadMeterDetail(id: number): void {
        this.isLoading = true;
        this.meterService.getMeterById(id).subscribe({
            next: (meter) => {
                this.meter = meter;
                this.isLoading = false;
            },
            error: () => {
                this.isLoading = false;
                this.snackBar.open('Error loading meter details', 'Close', { duration: 3000 });
            },
        });
    }

    loadMeterData(): void {
        this.isLoadingData = true;
        const startDate = this.fromDate.value || new Date(Date.now() - 24 * 60 * 60 * 1000);
        const endDate = this.toDate.value || new Date();
        const selectedColumns = this.columnControl.value || [];
        const intervalSeconds = this.intervalControl.value || 60;

        // Update table columns dynamically
        this.operatingColumns = ['timestamp', ...selectedColumns];

        this.meterApi.queryDataByTimeRange({
            meter_id: this.meterId,
            columns: selectedColumns,
            time_range: {
                start_utc: startDate.toISOString(),
                end_utc: endDate.toISOString()
            },
            limit: 5000,
            order: 'asc',
            interval_seconds: intervalSeconds
        }).subscribe({
            next: (response) => {
                this.apiDataPoints = response.data;
                this.isLoadingData = false;
                this.updateCharts();
            },
            error: (err) => {
                this.isLoadingData = false;
                this.snackBar.open(`Error loading data: ${err.message}`, 'Close', { duration: 5000 });
            },
        });
    }

    getColumnInfo(columnName: string): ColumnDefinition | undefined {
        return COLUMN_DEFINITIONS.find(c => c.column_name === columnName);
    }

    updateCharts(): void {
        if (this.apiDataPoints.length === 0) return;

        const labels = this.apiDataPoints.map(p => {
            const d = new Date(p['time_stamp']);
            return `${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}`;
        });

        const selectedColumns = this.columnControl.value || [];
        const datasets: any[] = [];

        const colors = [
            '#dc3545', '#28a745', '#0d6efd', '#ffc107', '#17a2b8',
            '#6610f2', '#e83e8c', '#fd7e14', '#20c997', '#6f42c1'
        ];

        selectedColumns.forEach((colName, index) => {
            const info = this.getColumnInfo(colName);
            const color = colors[index % colors.length];

            if (info) {
                const label = info.unit ? `${info.description} (${info.unit})` : info.description;
                datasets.push({
                    data: this.apiDataPoints.map(p => p[colName] || 0),
                    label: label,
                    borderColor: color,
                    backgroundColor: `${color}1A`, // 10% opacity
                    fill: false,
                    tension: 0.3,
                    pointRadius: 2,
                });
            }
        });

        // Voltage Chart (reused for all Operating Parameters)
        this.operatingChartData = {
            labels,
            datasets
        };

        this.updateLoadChart();
    }

    loadProfileData(): void {
        this.isLoadingData = true;
        const startDate = this.buildDateTime(this.fromDate.value, this.fromTime.value);
        const endDate = this.buildDateTime(this.toDate.value, this.toTime.value);
        const survey = this.loadSurveyControl.value || 'LS02';

        this.meterApi.readProfile({
            meter_id: this.meterId,
            survey,
            from_datetime: this.formatDateOnly(startDate),
            to_datetime: this.formatDateOnly(endDate),
            max_records: 1000,
        }).subscribe({
            next: (response) => {
                this.loadProfileDataPoints = response.data || [];
                this.loadProfileFields = response.field || [];
                this.loadSurveyLabel = response.survey || survey;
                this.loadSamplingIntervalLabel = this.calculateSamplingInterval(this.loadProfileDataPoints);
                const selected = (this.loadColumnControl.value || [])
                    .filter(field => this.loadProfileFields.includes(field));
                const nextSelection = selected.length > 0 ? selected : this.loadProfileFields;
                this.loadColumnControl.setValue(nextSelection, { emitEvent: false });
                this.loadColumns = ['timestamp', ...nextSelection];
                this.isLoadingData = false;
                this.updateLoadChart();
            },
            error: (err) => {
                this.isLoadingData = false;
                this.snackBar.open(`Error loading load profile: ${err.message}`, 'Close', { duration: 5000 });
            },
        });
    }

    private calculateSamplingInterval(points: MeterDataPoint[]): string {
        if (points.length < 2) return 'N/A';
        const first = new Date(points[0].time_stamp).getTime();
        const second = new Date(points[1].time_stamp).getTime();
        const diffSeconds = Math.abs(Math.round((second - first) / 1000));
        if (!diffSeconds) return 'N/A';
        if (diffSeconds % 3600 === 0) {
            const hours = diffSeconds / 3600;
            return `${hours}h`;
        }
        if (diffSeconds % 60 === 0) {
            const minutes = diffSeconds / 60;
            return `${minutes}m`;
        }
        return `${diffSeconds}s`;
    }

    updateLoadChart(): void {
        if (this.loadProfileDataPoints.length === 0) {
            this.loadChartData = { labels: [], datasets: [] };
            return;
        }

        const labels = this.loadProfileDataPoints.map(p => {
            const d = new Date(p['time_stamp']);
            return `${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}`;
        });

        const datasets: any[] = [];
        const selectedFields = this.loadColumnControl.value || [];

        const colors = [
            '#28a745', '#dc3545', '#0d6efd', '#ffc107', '#17a2b8',
            '#6610f2', '#e83e8c', '#fd7e14', '#20c997', '#6f42c1'
        ];

        selectedFields.forEach((field, index) => {
            const color = colors[index % colors.length];
            datasets.push({
                data: this.loadProfileDataPoints.map(p => p[field] ?? 0),
                label: field,
                borderColor: color,
                backgroundColor: `${color}1A`,
                fill: false,
                tension: 0.3,
                pointRadius: 2,
            });
        });

        this.loadChartData = { labels, datasets };
    }

    applyFilter(): void {
        this.snackBar.open('Loading data with filter...', '', { duration: 1500 });
        this.loadMeterData();
    }

    applyLoadProfileFilter(): void {
        this.snackBar.open('Loading load profile...', '', { duration: 1500 });
        this.loadProfileData();
    }

    exportExcel(type: string): void {
        const exportConfig = this.buildExportConfig(type);
        if (!exportConfig) {
            this.snackBar.open('Nothing to export yet. Load data first.', 'Close', { duration: 3000 });
            return;
        }

        const { filename, sheetName, rows } = exportConfig;
        const workbook = XLSX.utils.book_new();
        const worksheet = XLSX.utils.json_to_sheet(rows);
        XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);

        const arrayBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
        const blob = new Blob([arrayBuffer], {
            type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        });

        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        link.click();
        window.URL.revokeObjectURL(url);

        this.snackBar.open('Excel exported successfully!', 'Close', { duration: 3000 });
    }

    goBack(): void {
        this.router.navigate(['/dashboard']);
    }

    formatDateTime(date: Date | string | undefined): string {
        if (!date) return '-';
        const d = new Date(date);
        return `${d.toLocaleDateString('en-US')} ${d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}`;
    }

    formatNumber(value: number | undefined, decimals = 2): string {
        if (value === undefined || value === null) return '-';
        return value.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
    }

    onLoadProfileColumnsChange(): void {
        this.loadColumns = ['timestamp', ...(this.loadColumnControl.value || [])];
        this.updateLoadChart();
    }

    private buildDateTime(dateValue: Date | null, timeValue: string | null): Date {
        const base = dateValue ? new Date(dateValue) : new Date();
        const [hours, minutes] = (timeValue || '00:00').split(':').map(Number);
        base.setHours(hours || 0, minutes || 0, 0, 0);
        return base;
    }

    private formatLocalDateTime(date: Date): string {
        const pad = (value: number) => String(value).padStart(2, '0');
        const offsetMinutes = -date.getTimezoneOffset();
        const sign = offsetMinutes >= 0 ? '+' : '-';
        const absMinutes = Math.abs(offsetMinutes);
        const offsetHours = pad(Math.floor(absMinutes / 60));
        const offsetMins = pad(absMinutes % 60);
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}${sign}${offsetHours}:${offsetMins}`;
    }

    private buildExportConfig(type: string): { filename: string; sheetName: string; rows: Record<string, unknown>[] } | null {
        const dateTag = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
        const safeName = (value: string) => value.replace(/[^a-zA-Z0-9-_]/g, '_');
        const meterName = safeName(this.meter?.meterPointName || `meter_${this.meterId}`);

        if (type === 'operating') {
            if (!this.apiDataPoints.length) {
                return null;
            }
            const columns = ['timestamp', ...(this.columnControl.value || [])];
            const rows = this.apiDataPoints.map(point => this.mapRow(point, columns));
            return {
                filename: `${meterName}_operating_${dateTag}.xlsx`,
                sheetName: 'Operating',
                rows,
            };
        }

        if (type === 'load') {
            if (!this.loadProfileDataPoints.length) {
                return null;
            }
            const columns = this.loadColumns.length ? this.loadColumns : ['timestamp', ...this.loadProfileFields];
            const rows = this.loadProfileDataPoints.map(point => this.mapRow(point, columns));
            const surveyLabel = safeName(this.loadSurveyLabel || 'survey');
            return {
                filename: `${meterName}_${surveyLabel}_${dateTag}.xlsx`,
                sheetName: 'LoadProfile',
                rows,
            };
        }

        if (type === 'finalized') {
            if (!this.apiDataPoints.length) {
                return null;
            }
            const columns = this.finalizedColumns;
            const rows = this.apiDataPoints.map(point => this.mapRow(point, columns));
            return {
                filename: `${meterName}_finalized_${dateTag}.xlsx`,
                sheetName: 'Finalized',
                rows,
            };
        }

        return null;
    }

    private mapRow(point: MeterDataPoint, columns: string[]): Record<string, unknown> {
        const row: Record<string, unknown> = {};
        for (const column of columns) {
            if (column === 'timestamp') {
                row[column] = point.time_stamp ? new Date(point.time_stamp).toISOString() : '';
                continue;
            }
            row[column] = point[column] ?? '';
        }
        return row;
    }

    private formatTimeValue(date: Date): string {
        return `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
    }

    private formatDateOnly(date: Date): string {
        const pad = (value: number) => String(value).padStart(2, '0');
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
    }
}
