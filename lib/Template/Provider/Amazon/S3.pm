package Template::Provider::Amazon::S3;
# ABSTRACT: Enable template toolkit to use Amazon's S3 service as a provier of templates.
use base 'Template::Provider';

# use version 0.77; our $VERSION = version->declare("v0.0.1");

use Net::Amazon::S3::Client;
use DateTime;
use Try::Tiny;
use List::MoreUtils qw( uniq );

=head1 SYNOPSIS

   use Template;
   use Template::Provider::Amazon::S3;

   # Specify the provider in the config for Template::Toolkit. 
   # Note since the AWS ACCESS KEY, SECRET, and bucket name 
   # is not provided here, it will get it from the following 
   # Envrionmental variables:
   #  AWS_ACCESS_KEY_ID
   #  AWS_SECRET_ACCESS_KEY
   #  AWS_TEMPLATE_BUCKET
   my $tt_config = {
       LOAD_TEMPLATES => [
         Template::Provider::Amazon::S3->new( INCLUDE_PATH => [ 'dir1', 'dir2' ] )
       ]
   };

   my $tt = Template->new($tt_config);
   $tt->process('file_on_s3',$vars) || die $tt->error;

=head1 INHERITED METHODS

  These methods are inherited from Template::Provider and function in the same way.

=over 2

=item fetch()

=item store()

=item load()

=item include_path()

=item paths()

=item DESTROY()

=back

=head1 CLASS Methods

  $obj = $class->new( %parameters )

  constructs a new instance.

  Accepts all the arguments as the base class L<Template::Provider>, with the following additions:

=over 4

=item B<key>

  This is the Amazon Access key, if this is not provided we will try
  and load this from the AWS_ACCESS_KEY_ID environment variable.

=item B<secret>

  This is the Amazon Secret Key, if this is not provided we will try
  and load this from the AWS_ACCESS_KEY_SECRET environment variable.

=item B<bucketname>

  This is the bucket that will contain all the templates. If this it
  not provided we will try and get it from the AWS_TEMPLATE_BUCKET 
  envrionement variable. 
  
=item B<INCLUDE_PATH>

  This should be an array ref to directories that will be searched for the
  template. This method is really naive, and just prepends each entry to 
  the template name. 

=back


=head2 Note

  Note do not use the RELATIVE or the ABSOLUTE parameters, I don't know 
  what will happen if they are used. 

=cut

=method client

  This method will return the S3 client.

=cut

sub client {
 
  my $self = shift;
  return $self->{CLIENT} if $self->{CLIENT};
  my $s3 = Net::Amazon::S3->new(
      aws_access_key_id => $self->{ AWS_ACCESS_KEY_ID },
      aws_secret_access_key => $self->{ AWS_SECRET_ACCESS_KEY },
      retry => 1,
  );
  $self->{ CLIENT } = Net::Amazon::S3::Client->new( s3 => $s3 );

}

=method bucket

   This method will return the bucket that was configure in the begining.

=cut

sub bucket { 
   my $self = shift;
   return $self->{BUCKET} if $self->{BUCKET};
   return  unless $self->{ BUCKETNAME };
   my $client = $self->client;
   return unless $self->client;
   $self->{BUCKET} = $client->bucket( name => $self->{ BUCKETNAME } );
}
{
my %cache = ();
sub cache {
   my ($self, $key, $obj) = @_;
   $cache{$key} = $obj if $obj;
   $cache{$key}
};
}

=method refresh_cache

  Call this method to refresh the in memory store.

=cut 

sub refresh_cache {

   my $self = shift;
   my $key = shift;
   my $bucket = $self->bucket;
   return unless $bucket;
   my $stream = $bucket->list;
   until ( $stream->is_done ){
      foreach $object ( $stream->items ) {
         $self->cache( $object->key => $object );
      }
   }
   return unless $key and defined wantarray ;
   my @paths = $self->_get_paths($key);
   foreach my $path_key ( @paths ) {
       $obj = $self->cache( $path_key );
       return $obj if $obj;
   }
   return;
}

=method object

   returns the object for a given key. 
   This method take a key parameter.

     $obj = $self->object( key => 'some_path' );

=cut

sub _clean_up_path($) { join '/', grep { $_!~/\.{1,2}/ } split '/', shift };

sub _get_paths {
    my $self = shift;
    my $key = shift;
    my @paths = grep { defined } map { /^\s*$/ ? undef : $_  } uniq 
                 map { _clean_up_path $_ } ('', @{$self->include_path} );
    return ( $key , map { join '/',$_,$key } @paths ) 
}
   
sub object {
   my ($self, %args) = @_;
   my $key = $args{key};
   return unless $key;
   my @paths = $self->_get_paths($key);
   foreach my $path_key ( @paths ) {
       $obj = $self->cache( $path_key );
       return $obj if $obj;
   }
   return $self->refresh_cache( $key );
}

sub _init {
  my ( $self, $options ) = @_;
  $self->{ AWS_ACCESS_KEY_ID } = $options->{ key }          || $ENV{AWS_ACCESS_KEY_ID};
  $self->{ AWS_SECRET_ACCESS_KEY } = $options->{ secret } || $options->{ secrete } || $ENV{AWS_ACCESS_KEY_SECRET};
  $self->{ BUCKETNAME } = $options->{ bucketname }          || $ENV{AWS_TEMPLATE_BUCKET};
  $self->SUPER::_init($options);
}

sub _template_modified {
   my ($self, $template) = @_;
   $template =~s#^\./##;
   my $object;
   try {
      $object = $self->object( key => $template );
   } catch {
      return undef;
   };
   return unless $object;
   my $ldate = $object->last_modified || DateTime->now;
   $ldate->epoch;
}

sub _template_content {
   my ($self, $template) = @_;
   $template =~s#^\./##;
   return wantarray? (undef, 'No path specified to fetch content from')   : undef unless $template;
   return wantarray? (undef, 'No Bucket specified to fetch content from') : undef unless $self->bucket;
   my $object; 
   try {
      $object = $self->object( key => $template );
   } catch {
      return wantarray? (undef, 'AWS error: '.$_ ) : undef;
   };
   return wantarray? (undef, "object ($template) not found") : undef 
       unless $object && $object->exists;
   my $data = $object->get;
   my $ldate = $object->last_modified || DateTime->now;
   $mod_date = $ldate->epoch;
   return wantarray? ($data, undef, $mod_date) : $data;
}

=head1 SEE ALSO

=over 4 

=item L<Net::Amazon::S3::Client>

=item L<Net::Amazon::S3::Client::Bucket>

=item L<Net::Amazon::S3::Client::Object>

=back

=cut

1;
