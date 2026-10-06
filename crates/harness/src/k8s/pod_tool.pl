# The pod tool (crates/harness/src/k8s/remote.rs `POD_TOOL`): the system
# calls the k8s scenarios make inside workload pods that the driver image's
# coreutils/util-linux do not (it has perl-base, not attr or python):
# xattrs, fcntl locks, SEEK_HOLE/SEEK_DATA, an O_APPEND writer and its
# checker. `perl /tmp/pt.pl OP ARGS...`; every OP is documented where it
# is handled.
use strict;
use warnings;
use Fcntl qw(:DEFAULT :flock);
use IO::Handle;

my %XATTR = (x86_64 => [188, 191, 197], aarch64 => [5, 8, 14]);

sub fl { pack('s s x4 q q i x4', $_[0], 0, $_[1], $_[2], 0) }
my %TYPE = (r => F_RDLCK, w => F_WRLCK, u => F_UNLCK);

sub rw {
    sysopen(my $fh, $_[0], O_RDWR | O_CREAT) or die "open $_[0]: $!\n";
    return $fh;
}

sub touch { open(my $t, '>', $_[0]) or die "touch $_[0]: $!\n"; close $t }

sub await { select(undef, undef, undef, 0.05) until -e $_[0] }

sub lockw {
    my ($fh, $type, $start, $len) = @_;
    fcntl($fh, F_SETLKW, fl($TYPE{$type}, $start, $len)) or die "F_SETLKW: $!\n";
}

sub block {
    my ($seed, $i) = @_;
    my $b = chr(($seed & 0xff) ^ ($i & 0xff)) x 65536;
    substr($b, 0, 16) = pack('Q< Q<', $i, $seed);
    return $b;
}

my $op = shift @ARGV // die "usage\n";
if ($op =~ /^x(set|get|rm)$/) {
    chomp(my $m = `uname -m`);
    my $nr = $XATTR{$m} or die "no xattr syscall numbers for $m\n";
    my ($path, $name, $value) = @ARGV;
    if ($1 eq 'set') {
        syscall($nr->[0], $path, $name, $value, length($value), 0) == 0
            or die "setxattr $path $name: $!\n";
    } elsif ($1 eq 'get') {
        my $buf = "\0" x 65536;
        my $n = syscall($nr->[1], $path, $name, $buf, length($buf));
        if ($n < 0) { printf "ERRNO %d\n", $! + 0; exit 0 }
        print substr($buf, 0, $n);
    } else {
        syscall($nr->[2], $path, $name) == 0 or die "removexattr $path $name: $!\n";
    }
} elsif ($op eq 'seek') {
    # seek PATH OFFSET: SEEK_HOLE and SEEK_DATA from OFFSET.
    my ($path, $off) = @ARGV;
    open(my $fh, '<', $path) or die "open $path: $!\n";
    my $hole = sysseek($fh, $off, 4) // die "SEEK_HOLE: $!\n";
    my $data = sysseek($fh, $off, 3) // die "SEEK_DATA: $!\n";
    printf "%d %d\n", $hole, $data;
} elsif ($op eq 'try') {
    # try PATH TYPE START LEN: F_SETLK, `OK` or `ERR <errno>`.
    my ($path, $type, $start, $len) = @ARGV;
    my $fh = rw($path);
    if (fcntl($fh, F_SETLK, fl($TYPE{$type}, $start, $len))) { print "OK\n" }
    else { printf "ERR %d\n", $! + 0 }
} elsif ($op eq 'getlk') {
    # getlk PATH TYPE START LEN: the conflicting lock's type (`r`, `w`, `u`).
    my ($path, $type, $start, $len) = @ARGV;
    my $fh = rw($path);
    my $l = fl($TYPE{$type}, $start, $len);
    fcntl($fh, F_GETLK, $l) or die "F_GETLK: $!\n";
    my ($t) = unpack('s', $l);
    my %name = reverse %TYPE;
    print "$name{$t}\n";
} elsif ($op eq 'hold') {
    # hold PATH TYPE START LEN DATA HELD RELEASE: F_SETLKW, write DATA at 0
    # under the lock (if any) and fsync, touch HELD, wait for RELEASE, unlock.
    my ($path, $type, $start, $len, $data, $held, $release) = @ARGV;
    my $fh = rw($path);
    lockw($fh, $type, $start, $len);
    if (length $data) {
        sysseek($fh, 0, 0);
        syswrite($fh, $data) == length($data) or die "write: $!\n";
        $fh->sync or die "fsync: $!\n";
    }
    touch($held);
    await($release);
    fcntl($fh, F_SETLK, fl(F_UNLCK, $start, $len)) or die "F_UNLCK: $!\n";
    close $fh;
} elsif ($op eq 'lockread') {
    # lockread PATH START LEN N: F_SETLKW a write lock, print the first N bytes.
    my ($path, $start, $len, $n) = @ARGV;
    my $fh = rw($path);
    lockw($fh, 'w', $start, $len);
    sysseek($fh, 0, 0);
    my $got = sysread($fh, my $buf, $n) // die "read: $!\n";
    print $buf;
    fcntl($fh, F_SETLK, fl(F_UNLCK, $start, $len)) or die "F_UNLCK: $!\n";
} elsif ($op eq 'incr') {
    # incr PATH N: N times, under a whole-file write lock: read the
    # counter, write it plus one in place, fsync, unlock.
    my ($path, $n) = @ARGV;
    my $fh = rw($path);
    for (1 .. $n) {
        lockw($fh, 'w', 0, 0);
        sysseek($fh, 0, 0);
        sysread($fh, my $buf, 64) // die "read: $!\n";
        my $v = ($buf =~ /^(\d+)/) ? $1 : 0;
        my $out = sprintf("%d\n", $v + 1);
        sysseek($fh, 0, 0);
        syswrite($fh, $out) == length($out) or die "write: $!\n";
        truncate($fh, length $out) or die "truncate: $!\n";
        $fh->sync or die "fsync: $!\n";
        fcntl($fh, F_SETLK, fl(F_UNLCK, 0, 0)) or die "F_UNLCK: $!\n";
    }
} elsif ($op eq 'append') {
    # append PATH SEED SECS MAX: 64 KiB blocks through one O_APPEND
    # descriptor for SECS seconds or MAX blocks; its close checked; prints
    # the block count.
    my ($path, $seed, $secs, $max) = @ARGV;
    sysopen(my $fh, $path, O_WRONLY | O_APPEND | O_CREAT) or die "open $path: $!\n";
    my ($end, $i) = (time + $secs, 0);
    while (time < $end && $i < $max) {
        my $b = block($seed, $i);
        my $off = 0;
        while ($off < length $b) {
            my $w = syswrite($fh, $b, length($b) - $off, $off);
            die "append $i of $path failed: $!\n" unless defined $w;
            $off += $w;
        }
        $i++;
    }
    close($fh) or die "close() of the appending descriptor of $path failed: $!\n";
    print "$i\n";
} elsif ($op eq 'verify') {
    # verify PATH SEED BLOCKS: `OK`, or the first block out of place.
    my ($path, $seed, $blocks) = @ARGV;
    open(my $fh, '<', $path) or die "open $path: $!\n";
    binmode $fh;
    my ($size, $want) = (-s $path, $blocks * 65536);
    for my $i (0 .. $blocks - 1) {
        my $got = read($fh, my $buf, 65536) // die "read: $!\n";
        next if $got == 65536 && $buf eq block($seed, $i);
        my $found = $got == 65536 ? 'block ' . unpack('Q<', $buf) : "$got bytes";
        printf "%s is %d bytes, %d acknowledged (%d short); block %d of %d (offset %d) holds %s\n",
            $path, $size, $want, $want - $size, $i, $blocks, $i * 65536, $found;
        exit 0;
    }
    print $size == $want ? "OK\n" : "$path is $size bytes, $want acknowledged\n";
} else {
    die "unknown op $op\n";
}
