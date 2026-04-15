import { Injectable, Inject, PLATFORM_ID } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';
import { BehaviorSubject, Observable, catchError, map, of, switchMap } from 'rxjs';
import { isPlatformBrowser } from '@angular/common';
import { MatSnackBar } from '@angular/material/snack-bar';
import { User, LoginCredentials, LoginResponse, DEFAULT_PERMISSIONS } from '../interfaces/user.interface';
import { environment } from '../../environments/environment';

@Injectable({
    providedIn: 'root',
})
export class AuthService {
    private isLoggedInSubject = new BehaviorSubject<boolean>(false);
    private currentUserSubject = new BehaviorSubject<User | null>(null);

    public isLoggedIn$ = this.isLoggedInSubject.asObservable();
    public currentUser$ = this.currentUserSubject.asObservable();

    private apiUrl = environment.apiUrl;

    constructor(
        private http: HttpClient,
        private router: Router,
        private snackBar: MatSnackBar,
        @Inject(PLATFORM_ID) private platformId: Object
    ) {
        this.checkSession();
    }

    public get isLoggedIn(): boolean {
        return this.isLoggedInSubject.value;
    }

    public get currentUser(): User | null {
        return this.currentUserSubject.value;
    }

    private checkSession(): void {
        if (!isPlatformBrowser(this.platformId)) {
            return;
        }

        const token = localStorage.getItem('access_token');
        if (token) {
            // Attempt to fetch user profile with stored token
            this.fetchUserProfile().subscribe({
                next: (user) => {
                    this.isLoggedInSubject.next(true);
                    this.currentUserSubject.next(user);
                },
                error: () => this.clearSession()
            });
        }
    }

    /**
     * Fetch the user profile from the backend using the current token
     */
    private fetchUserProfile(): Observable<User> {
        return this.http.get<User>(`${this.apiUrl}/auth/me`);
    }

    /**
     * Login with credentials via the backend API
     */
    login(credentials: LoginCredentials): Observable<LoginResponse> {
        const formData = new URLSearchParams();
        formData.set('username', credentials.username);
        formData.set('password', credentials.password);

        return this.http.post<{ access_token: string, token_type: string }>(
            `${this.apiUrl}/auth/login`,
            formData.toString(),
            {
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
            }
        ).pipe(
            switchMap(res => {
                if (isPlatformBrowser(this.platformId)) {
                    localStorage.setItem('access_token', res.access_token);
                }
                return this.fetchUserProfile().pipe(
                    map(user => {
                        this.startSession(user);
                        return {
                            success: true,
                            user: user,
                            message: 'Login successful'
                        };
                    })
                );
            }),
            catchError(error => {
                return of({
                    success: false,
                    message: error.error?.detail || 'Invalid username or password'
                });
            })
        );
    }

    logout(): void {
        this.clearSession();
        this.router.navigate(['/login']);
        this.showMessage('Logged out', 'info');
    }

    private startSession(user: User): void {
        this.isLoggedInSubject.next(true);
        this.currentUserSubject.next(user);
    }

    private clearSession(): void {
        if (isPlatformBrowser(this.platformId)) {
            localStorage.removeItem('access_token');
        }
        this.isLoggedInSubject.next(false);
        this.currentUserSubject.next(null);
    }

    public showMessage(message: string, type: 'success' | 'error' | 'warning' | 'info'): void {
        this.snackBar.open(message, 'Close', {
            duration: 4000,
            panelClass: [`${type}-snackbar`],
            horizontalPosition: 'end',
            verticalPosition: 'top',
        });
    }
}
